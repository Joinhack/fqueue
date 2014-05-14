package fqueue

import (
	"encoding/binary"
	"math/rand"
	"os"
	"sync"
	"testing"
	"path/filepath"
	"time"
)

func TestFQueue(t *testing.T) {
	var fq *FQueue
	var err error
	FileLimit = PageSize * 2
	fpath := filepath.Join(os.TempDir(), "fq1.data")
	os.Remove(fpath)
	if fq, err = NewFQueue(fpath); err != nil {
		panic(err)
	}
	var wg = &sync.WaitGroup{}
	wg.Add(2)
	var total = 0
	var d []byte
	var limit = 100000
	startTime := time.Now()
	go func() {
		var err error
		for i := 0; i < limit; {
			l := rand.Intn(256) + 8
			d = make([]byte, l)
			total += l
			binary.LittleEndian.PutUint32(d, uint32(i))
			binary.LittleEndian.PutUint32(d[4:], uint32(l))

			if err = fq.Push(d); err != nil {
				if err == NoSpace {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				panic(err)
			}
			i++
		}
		wg.Done()
	}()
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
					t.Fatalf("the seq error")
					return
				}
				// println(c, l)
				i++
			} else {
				t.Fatalf("the test data mustbe greater than 8")
				return
			}
		}
		wg.Done()
	}()

	wg.Wait()
	endTime := time.Now()
	t.Log("total read write bytes:", total, "speed(bytes/s):", total/int(endTime.Unix()-startTime.Unix()), ",speed(bytes/ms):", total/int((endTime.UnixNano()-startTime.UnixNano())/1000000))
	fq.Close()
	os.Remove(fpath)
}
