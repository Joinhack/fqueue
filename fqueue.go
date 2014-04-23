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

var (
	Timeout    = errors.New("operation timeout")
	QueueEmpty = errors.New("queue is empty")
	magic      = "JFQ"
)

type meta struct {
	WriterOffset int
	ReaderOffset int
	ReaderPtr    int
	WriterBottom int
	Free         int
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
	qMutex   *sync.Mutex
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
	println("Free:", m.Free)
	println("Limit:", m.Limit)
	println("ReaderOffset:", m.ReaderOffset)
	println("ReaderPtr:", m.ReaderPtr)
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
	var needSpace = plen + 16
	for i := 0; i < 100 && q.Free < needSpace; i++ {
		q.qMutex.Unlock()
		//the queue is full, wait for consume
		time.Sleep(10 * time.Millisecond)
		q.qMutex.Lock()
	}
	if q.Free < needSpace {
		return Timeout
	}
	var l uint16 = uint16(len(p))
	if err = binary.Write(q, binary.LittleEndian, l); err != nil {
		return err
	}
	if err = binary.Write(q, binary.LittleEndian, p); err != nil {
		return err
	}
	if err = q.Flush(); err != nil {
		return err
	}

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
	fileLimit := cfg.FileLimit

	q := &FQueue{
		qMutex: &sync.Mutex{},
	}
	q.Free = fileLimit
	q.Limit = fileLimit

	q.metaFd, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		if os.IsExist(err) {
			//TODO: load
		}
		return
	} else {
		q.meta.ReaderOffset = 1024
		q.meta.WriterOffset = 1024
		q.meta.ReaderPtr = 1024
		q.dumpMeta()
	}

	if q.Reader, err = NewReader(path, q); err != nil {
		return
	}
	if q.Writer, err = NewWriter(path, q); err != nil {
		return
	}
	fq = q
	return

}

func (q *FQueue) dumpMeta() error {
	var buf [1024]byte
	var p = buf[:]
	var offset = 0
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	copy(p[0:len(magic)], []byte(magic))
	offset += len(magic)
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.Free))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.Limit))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.WriterBottom))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.WriterOffset))
	offset += 8
	binary.LittleEndian.PutUint64(p[offset:], uint64(q.meta.ReaderPtr))
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
	if err = binary.Read(q, binary.LittleEndian, &l); err != nil {
		if err == io.EOF {
			err = QueueEmpty
			return
		}
		return
	}
	p = make([]byte, l)
	if err = binary.Read(q, binary.LittleEndian, p); err != nil {
		if err == io.EOF {
			err = QueueEmpty
			return
		}
		return
	}
	q.ReaderPtr += int(2 + l)
	if q.ReaderPtr == q.WriterBottom && q.WriterOffset < q.WriterBottom {
		q.WriterBottom = q.WriterOffset
	}
	return
}
