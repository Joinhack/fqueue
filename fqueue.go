package fqueue

import (
	"container/list"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"time"
)

const (
	MetaSize = 1024
)

var (
	NoSpace     = errors.New("no space error")
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
	fSize        int
	MemLimit     int
}

var (
	FileLimit = 1024 * 1024 * 1024
	MemLimit  = 4096 * 1024 * 2
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
	err           error
}

func (q *FQueue) getMeta() *meta {
	return &q.meta
}

func (q *FQueue) printMeta() {
	m := q.getMeta()
	println("File Contents:", m.fSize)
	println("Limit:", m.Limit)
	println("ReaderOffset:", m.ReaderOffset)
	println("WriterOffset:", m.WriterOffset)
	println("WriterBottom:", m.WriterBottom)
}

func (q *FQueue) flush() {
	if q.err != nil {
		return
	}
	for e := q.memQueue.Front(); e != nil; e = e.Next() {
		p := e.Value.([]byte)
		var plen = len(p)
		if q.err = binary.Write(q, binary.LittleEndian, uint16(plen)); q.err != nil {
			return
		}
		if _, q.err = q.Write(p); q.err != nil {
			return
		}
		q.fSize += (2 + plen)
		q.WriterOffset += 2 + plen
		q.Writer.setBottom()
		if q.WriterOffset > q.Limit {
			if q.err = q.Writer.Flush(); q.err != nil {
				return
			}
			q.err = q.Writer.rolling()
		}
	}
	if q.err = q.Writer.Flush(); q.err != nil {
		return
	}
	// if q.err = q.Writer.fd.Sync(); q.err != nil {
	// 	return
	// }
	q.memQueue.Init()
	q.mSize = 0
}

func (q *FQueue) needFlush() {
	now := time.Now().Unix()
	if q.mSize >= q.Limit-MetaSize || q.mSize >= q.MemLimit || (now-q.lastFlushTime >= 1 && q.mSize > 0) {
		q.flush()
		q.lastFlushTime = now
	}
}

func (q *FQueue) Push(p []byte) error {
	var plen = len(p)
	if q.err != nil {
		return q.err
	}
	if plen == 0 {
		return nil
	}
	q.qMutex.Lock()
	defer q.qMutex.Unlock()

	var needSpace = plen + 2
	if q.mSize+needSpace > q.Limit-MetaSize-q.fSize {
		return NoSpace
	}
	if q.fSize+needSpace > q.Limit-MetaSize {
		return NoSpace
	}
	q.mSize += needSpace
	q.memQueue.PushBack(p)
	q.needFlush()
	return nil
}

func NewFQueue(path string) (fq *FQueue, err error) {
	fileLimit := FileLimit + MetaSize //1024 is meta

	memLimit := MemLimit

	q := &FQueue{
		qMutex:   &sync.Mutex{},
		wg:       &sync.WaitGroup{},
		memQueue: list.New(),
	}
	q.Limit = fileLimit
	q.fSize = 0
	q.MemLimit = memLimit
	var st os.FileInfo
	if st, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			q.metaFd, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
			q.meta.ReaderOffset = MetaSize
			q.meta.WriterOffset = MetaSize
			q.dumpMeta()
			if err != nil {
				return
			}
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
		q.needFlush()
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
	q.meta.fSize = int(binary.LittleEndian.Uint64(p[offset:]))
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
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.fSize))
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
	if q.fSize == 0 {
		if q.mSize == 0 {
			err = QueueEmpty
			return
		}
		e := q.memQueue.Front()
		p = e.Value.([]byte)
		q.memQueue.Remove(e)
		q.mSize -= 2 + len(p)
		return
	}

	//check again
	if q.ReaderOffset == q.WriterBottom && q.WriterOffset < q.WriterBottom {
		q.WriterBottom = q.WriterOffset
		if err = q.Reader.rolling(); err != nil {
			return
		}
	}

	var lbuf [2]byte
	var n, c int
	for c < len(lbuf) {
		if n, err = q.Read(lbuf[:]); err != nil {
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
	q.fSize -= int(2 + l)
	return
}
