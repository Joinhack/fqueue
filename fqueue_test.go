package fqueue

import (
	"encoding/binary"
	//"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestFQueue(t *testing.T) {
	var fq *FQueue
	var err error
	FileLimit = 1024 * 15
	os.Remove("/tmp/fq1.data")
	if fq, err = NewFQueue("/tmp/fq1.data"); err != nil {
		panic(err)
	}
	var wg = &sync.WaitGroup{}
	wg.Add(1)
	var total = 0
	var d []byte
	var limit = 10000000

	go func() {
		var err error
		for i := 0; i < limit; {
			l := i%100 + 8
			d = make([]byte, l)
			total += l
			binary.LittleEndian.PutUint32(d, uint32(i))
			binary.LittleEndian.PutUint32(d[4:], uint32(l))

			if err = fq.Push(d); err != nil {
				if err == NoSpace {
					runtime.Gosched()
					continue
				}
				t.Fail()
			}
			i++
		}
		println("write finished~!")
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		i := 0
		for i < limit {
			var p []byte
			if p, err = fq.Pop(); err != nil {
				if err == QueueEmpty {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				fq.printMeta()
				panic(err)
			}
			if len(p) >= 8 {
				c := int(binary.LittleEndian.Uint32(p))
				l := int(binary.LittleEndian.Uint32(p[4:]))
				if c != i || l != len(p) {
					t.Fail()
				}
				i++
			} else {
				fq.printMeta()
				panic(len(p))
			}
		}
		wg.Done()
	}()

	wg.Wait()
	println("total read write bytes:", total)
	fq.Close()
}
