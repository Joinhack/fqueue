package fqueue

import (
	"os"
)

var (
	ChunkSize = 1024 * 1024 * 512
)

type ChunksQueue struct {
	chunks []*FQueue
	rIdx   int
	wIdx   int
}

func (q *ChunksQueue) Push(p []byte) error {
	return nil
}

func (q *ChunksQueue) Pop() (p []byte, err error) {
	return
}

func NewChunksQueue(path string) (q Queue, err error) {
	var info os.FileInfo
	if info, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(path, 0775); err != nil {
				return
			}
		} else {
			return
		}
	}
	if !info.IsDir() {
		err = MustBeDirectory
		return
	}
	
	return
}
