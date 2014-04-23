package fqueue

import (
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestFQueue(t *testing.T) {
	var fq *FQueue
	var err error
	FileLimit = 1024 * 10
	os.Remove("/tmp/fq1.data")
	if fq, err = NewFQueue(&FQueueCfg{Path: "/tmp/fq1.data"}); err != nil {
		panic(err)
	}
	var wg = &sync.WaitGroup{}
	wg.Add(1)

	var d = make([]byte, 1000)
	for i := 0; i < len(d); i++ {
		d[i] = byte(i % 255)
	}
	go func() {
		var err error
		for i := 0; i < 100000; i++ {
			if err = fq.Push(d); err != nil {
				panic(err)
			}
			runtime.Gosched()
		}
		println("write finished~!")
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		i := 0
		for i < 100000 {
			var p []byte
			if p, err = fq.Pop(); err != nil {
				if err == QueueEmpty {
					t.Log("empty queue")
					time.Sleep(2 * time.Millisecond)
					continue
				}
				panic(err)
			}
			if len(p) == len(d) {
				i++
			} else {
				panic(len(p))
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

func TestWriteTimeout(t *testing.T) {
	var fq *FQueue
	var err error
	FileLimit = 1024 * 1024
	os.Remove("/tmp/fq2.data")
	if fq, err = NewFQueue(&FQueueCfg{Path: "/tmp/fq2.data"}); err != nil {
		panic(err)
	}
	var wg = &sync.WaitGroup{}
	wg.Add(1)

	var d = make([]byte, 1000)
	for i := 0; i < len(d); i++ {
		d[i] = byte(i % 255)
	}

	for i := 0; i < 10000; i++ {
		if err = fq.Push(d); err != nil {
			if err != Timeout {
				t.Fail()
			}
			break
		}
	}

}

func TestWriteTimeout2(t *testing.T) {
	var fq *FQueue
	var err error
	FileLimit = 1024 * 10
	os.Remove("/tmp/fq3.data")
	if fq, err = NewFQueue(&FQueueCfg{Path: "/tmp/fq3.data"}); err != nil {
		panic(err)
	}
	var wg = &sync.WaitGroup{}
	wg.Add(1)

	var d = make([]byte, 1000)
	for i := 0; i < len(d); i++ {
		d[i] = byte(i % 255)
	}
	var r = true
	go func() {
		var err error
		for i := 0; i < 10000; i++ {
			if err = fq.Push(d); err != nil {
				if err == Timeout {
					r = false
					fq.printMeta()
					break
				}
				panic(err)
			}
		}
		println("write finished~!")
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		i := 0
		for i < 10000 && r {
			var p []byte
			if p, err = fq.Pop(); err != nil {
				if err == QueueEmpty {
					t.Log("empty queue")
					continue
				}
				panic(err)
			}
			time.Sleep(500 * time.Millisecond)
			if len(p) == len(d) {
				i++
			} else {
				panic(len(p))
			}
		}
		wg.Done()
	}()

	wg.Wait()
}
