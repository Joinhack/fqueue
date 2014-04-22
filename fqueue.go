package fqueue

import (
	"container/list"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

var (
	Timeout = errors.New("operation timeout")
)

const (
	FileLimit = 1024 * 1024 * 1024
)

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
}

type FQueue struct {
	*Writer
	*Reader
	fspace_free int
	memQueue    *list.List
	qMutex      *sync.Mutex
}

type FQueueCfg struct {
	Path      string
	FileLimit int
}

func (q *FQueue) Push(p []byte) error {
	var err error
	if len(p) == 0 {
		return nil
	}
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	for i := 0; i < 100 && q.fspace_free <= 0; i++ {
		q.qMutex.Unlock()
		time.Sleep(10 * time.Millisecond)
		q.qMutex.Lock()
	}
	if q.fspace_free <= 0 {
		return Timeout
	}
	var l uint16 = uint16(len(p))
	if err = binary.Write(q, binary.LittleEndian, l); err != nil {
		return err
	}
	if err = binary.Write(q, binary.LittleEndian, p); err != nil {
		return err
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
		qMutex:      &sync.Mutex{},
		fspace_free: fileLimit,
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

func (q *FQueue) Pop() (p []byte, err error) {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	var l uint16
	if err = binary.Read(q, binary.LittleEndian, &l); err != nil {
		if err == io.EOF {
			err = nil
			return
		}
		return
	}
	p = make([]byte, l)
	if err = binary.Read(q, binary.LittleEndian, p); err != nil {
		if err == io.EOF {
			err = nil
			return
		}
		return
	}
	return
}
