package fqueue

import (
	"sync"
	"testing"
	"time"
)

func TestFQueue(t *testing.T) {
	var fq *FQueue
	var err error
	if fq, err = NewFQueue(&FQueueCfg{Path: "/tmp/fq.data"}); err != nil {
		panic(err)
	}
	var wg = &sync.WaitGroup{}
	wg.Add(1)
	var d = make([]byte, 1024)
	for i := 0; i < len(d); i++ {
		d[i] = byte(i % 255)
	}
	go func() {
		var err error
		for i := 0; i < 1000000; i++ {
			if err = fq.Push(d); err != nil {
				panic(err)
			}
		}

		if err = fq.Flush(); err != nil {
			panic(err)
		}
		t.Log("write finished~!")
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		i := 0
		for i < 1000000 {
			var p []byte
			if p, err = fq.Pop(); err != nil {
				panic(err)
			}
			if len(p) > 0 {
				i++
			} else {
				t.Log("wait write")
				time.Sleep(1 * time.Millisecond)
			}
		}
		wg.Done()
	}()

	wg.Wait()
}
