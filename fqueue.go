package fqueue

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	PageSize = 4096
	MetaSize = PageSize
)

var (
	NoSpace     = errors.New("no space error")
	MunMapErr   = errors.New("munmap error")
	QueueEmpty  = errors.New("queue is empty")
	InvalidMeta = errors.New("invalid meta")
	MustBeFile  = errors.New("must be file")
	magic       = "JFQ"
)

type meta struct {
	WriterOffset int
	ReaderOffset int
	WriterBottom int
	Limit        int
	FSize        int
}

var (
	FileLimit = 1024 * 1024 * 1024
)

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
}

type FQueue struct {
	meta
	*Writer
	*Reader
	mSize         int
	lastFlushTime int64
	metaFd        *os.File
	memQueue      *list.List
	running       bool
	qMutex        *sync.Mutex
	wg            *sync.WaitGroup
}

func (q *FQueue) getMeta() *meta {
	return &q.meta
}

func (q *FQueue) printMeta() {
	m := q.getMeta()
	println("File Contents:", m.FSize)
	println("Limit:", m.Limit)
	println("ReaderOffset:", m.ReaderOffset)
	println("WriterOffset:", m.WriterOffset)
	println("WriterBottom:", m.WriterBottom)
}

func (q *FQueue) Push(p []byte) error {
	var plen = len(p)
	if plen == 0 {
		return nil
	}
	var err error
	q.qMutex.Lock()
	defer q.qMutex.Unlock()

	var needSpace = plen + 2
	if needSpace+q.FSize > q.Limit-MetaSize {
		return NoSpace
	}
	if (q.WriterOffset < q.WriterBottom) && q.WriterOffset+needSpace > q.ReaderOffset {
		return NoSpace
	}
	if err = binary.Write(q, binary.LittleEndian, uint16(plen)); err != nil {
		return err
	}
	if _, err = q.Write(p); err != nil {
		return err
	}
	q.FSize += (2 + plen)
	q.WriterOffset += 2 + plen
	q.Writer.setBottom()
	if q.WriterOffset >= q.Limit {
		err = q.Writer.rolling()
	}
	return err
}

func (q *FQueue) prepareQueueFile() {
	empty := make([]byte, 4096)
	n := time.Now()
	fmt.Println("prepared queue file")
	for i := 0; i < q.Limit; {
		q.metaFd.Write(empty)
		i += len(empty)
	}
	fmt.Println("prepared queue file, used time:", (time.Now().UnixNano()-n.UnixNano())/1000000, "ms")
}

func NewFQueue(path string) (fq *FQueue, err error) {
	fileLimit := FileLimit + MetaSize //1024 is meta

	q := &FQueue{
		qMutex:   &sync.Mutex{},
		wg:       &sync.WaitGroup{},
		memQueue: list.New(),
	}
	q.Limit = fileLimit
	q.FSize = 0
	var st os.FileInfo
	if st, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {

			q.metaFd, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

			q.meta.ReaderOffset = MetaSize
			q.meta.WriterOffset = MetaSize
			q.meta.WriterBottom = q.meta.WriterOffset
			q.dumpMeta()
			if err != nil {
				return
			}
			q.prepareQueueFile()
		} else {
			return
		}
	} else {
		if st.IsDir() {
			err = MustBeFile
			return
		}
		if st.Size() < MetaSize {
			err = InvalidMeta
			return
		}
		if err = q.loadMeta(path); err != nil {
			return
		}
	}

	if q.Reader, err = NewReader(path, q); err != nil {
		return
	}
	if q.Writer, err = NewWriter(path, q); err != nil {
		return
	}
	fq = q
	fq.running = true
	q.wg.Add(1)
	go fq.task()
	return
}

func (q *FQueue) Close() error {
	q.running = false
	q.wg.Wait()
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	if err := q.metaFd.Close(); err != nil {
		return err
	}
	if err := q.Writer.Close(); err != nil {
		return err
	}
	if err := q.Reader.Close(); err != nil {
		return err
	}
	return nil
}

func (q *FQueue) task() {
	for q.running {
		select {
		case <-time.After(1 * time.Second):
		}
		q.qMutex.Lock()
		q.dumpMeta()
		q.qMutex.Unlock()
	}
	q.wg.Done()
}

func (q *FQueue) loadMeta(path string) error {
	var buf [MetaSize]byte
	var err error
	q.metaFd, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	_, err = q.metaFd.Read(buf[:])
	if err != nil {
		return err
	}
	p := buf[:]
	offset := 0
	if string(p[:len(magic)]) != magic {
		return InvalidMeta
	}

	offset += len(magic)
	q.meta.FSize = int(binary.LittleEndian.Uint64(p[offset:]))
	offset += 8
	q.meta.Limit = int(binary.LittleEndian.Uint64(p[offset:]))
	offset += 8
	q.meta.WriterBottom = int(binary.LittleEndian.Uint64(p[offset:]))
	offset += 8
	q.meta.WriterOffset = int(binary.LittleEndian.Uint64(p[offset:]))
	offset += 8
	q.meta.ReaderOffset = int(binary.LittleEndian.Uint64(p[offset:]))
	return nil
}

func (q *FQueue) dumpMeta() error {
	var buf [MetaSize]byte
	var p = buf[:]
	var offset = 0
	if _, err := q.metaFd.Seek(0, os.SEEK_SET); err != nil {
		return err
	}
	copy(p[0:len(magic)], []byte(magic))
	offset += len(magic)
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.FSize))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.Limit))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.WriterBottom))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.WriterOffset))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.ReaderOffset))
	if _, err := q.metaFd.Write(buf[:]); err != nil {
		return err
	}
	return nil
}

func (q *FQueue) Pop() (p []byte, err error) {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	var l uint16
	if q.FSize == 0 {
		err = QueueEmpty
		return
	}
	if q.ReaderOffset == q.WriterBottom && q.WriterOffset < q.WriterBottom {
		q.WriterBottom = q.WriterOffset
		if err = q.Reader.rolling(); err != nil {
			return
		}
	}

	var lbuf [2]byte
	var n, c int
	c = 0
	for c < len(lbuf) {
		if n, err = q.Read(lbuf[c:]); err != nil {
			return
		}
		c += n
	}
	l = binary.LittleEndian.Uint16(lbuf[:])
	p = make([]byte, l)
	c = 0
	n = 0
	for c < int(l) {
		if n, err = q.Read(p[c:]); err != nil {
			return
		}
		c += n
	}

	q.ReaderOffset += int(2 + l)
	q.FSize -= int(2 + l)
	return
}
