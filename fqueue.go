package fqueue

import (
	"container/list"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

const (
	MetaSize = 1024
)

var (
	NoSpace    = errors.New("no space error")
	QueueEmpty = errors.New("queue is empty")
	magic      = "JFQ"
)

type meta struct {
	WriterOffset int
	ReaderOffset int
	WriterBottom int
	cBytes       int
	Limit        int
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
	metaFd   *os.File
	memQueue *list.List
	running  bool
	qMutex   *sync.Mutex
	wg       *sync.WaitGroup
}

type FQueueCfg struct {
	Path      string
	FileLimit int
}

func (q *FQueue) getMeta() *meta {
	return &q.meta
}

func (q *FQueue) printMeta() {
	m := q.getMeta()
	println("Contents:", m.cBytes)
	println("Limit:", m.Limit)
	println("ReaderOffset:", m.ReaderOffset)
	println("WriterOffset:", m.WriterOffset)
	println("WriterBottom:", m.WriterBottom)
}

func (q *FQueue) Push(p []byte) error {
	var err error
	var plen = len(p)
	if plen == 0 {
		return nil
	}
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	var needSpace = plen + 2
	if q.cBytes+needSpace > q.Limit-MetaSize {
		return NoSpace
	}
	if err = binary.Write(q, binary.LittleEndian, uint16(plen)); err != nil {
		return err
	}
	if err = binary.Write(q, binary.LittleEndian, p); err != nil {
		return err
	}
	if err = q.Flush(); err != nil {
		return err
	}
	q.cBytes += (2 + plen)
	q.WriterOffset += 2 + plen
	q.Writer.setBottom()
	if q.WriterOffset > q.Limit {
		q.Writer.rolling()
	}
	return nil
}

func NewFQueue(cfg *FQueueCfg) (fq *FQueue, err error) {
	path := cfg.Path
	if cfg.FileLimit == 0 {
		cfg.FileLimit = FileLimit
	}
	fileLimit := cfg.FileLimit + MetaSize //1024 is meta

	q := &FQueue{
		qMutex: &sync.Mutex{},
		wg:     &sync.WaitGroup{},
	}
	q.Limit = fileLimit
	q.cBytes = 0

	q.metaFd, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		if os.IsExist(err) {
			//TODO: load
		}
		return
	} else {
		q.meta.ReaderOffset = MetaSize
		q.meta.WriterOffset = MetaSize
		q.dumpMeta()
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
	go fq.dumpMetaTask()
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

func (q *FQueue) dumpMetaTask() {
	for q.running {
		select {
		case <-time.After(1 * time.Second):
		}
		q.dumpMeta()
	}
	q.wg.Done()
}

func (q *FQueue) dumpMeta() error {
	var buf [MetaSize]byte
	var p = buf[:]
	var offset = 0
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	copy(p[0:len(magic)], []byte(magic))
	offset += len(magic)
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.cBytes))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.Limit))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.WriterBottom))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.WriterOffset))
	offset += 8
	if _, err := q.metaFd.Seek(0, os.SEEK_SET); err != nil {
		return err
	}
	if _, err := q.metaFd.Write(buf[:]); err != nil {
		return err
	}
	return nil
}

func (q *FQueue) Pop() (p []byte, err error) {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	var l uint16
	if q.cBytes == 0 {
		err = QueueEmpty
		return
	} else {
		//check again
		if q.ReaderOffset == q.WriterBottom {
			q.WriterBottom = q.WriterOffset
			q.Reader.rolling()
		}
	}

	if err = binary.Read(q, binary.LittleEndian, &l); err != nil {
		if err == io.EOF {
			err = QueueEmpty
			return
		}
		return
	}
	p = make([]byte, l)
	if err = binary.Read(q, binary.LittleEndian, p); err != nil {
		return
	}
	q.ReaderOffset += int(2 + l)
	q.cBytes -= int(2 + l)

	return
}
