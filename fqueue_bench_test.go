package fqueue

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

var fpath string


func BenchmarkPush(b *testing.B) {
	b.N = 1000000
	var err error
	var p []byte
	var fq Queue
	fpath = filepath.Join(os.TempDir(), "fq1_benchmark.data")
	os.Remove(fpath)
	QueueLimit = 1024*1024*256
	
	if fq, err = NewFQueue(fpath); err != nil {
		panic(err)
	}
	defer fq.Close()
	ptr := make([]byte, 256 + 8)
	for i := 0; i < 256 + 8; i++ {
		ptr[i] = 'T'
	}
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		l := rand.Intn(256) + 8
		p = ptr[:l]
		b.SetBytes(int64(l))
		binary.LittleEndian.PutUint32(p, uint32(i))
		binary.LittleEndian.PutUint32(p[4:], uint32(l))

		if err = fq.Push(p); err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
	b.StopTimer()
	
}

func BenchmarkPop(b *testing.B) {
	b.N = 1000000
	var err error
	var p []byte
	var fq Queue
	if fq, err = NewFQueue(fpath); err != nil {
		panic(err)
	}
	defer fq.Close()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if p, err = fq.Pop(); err != nil {
			b.FailNow()
			return
		}
		b.SetBytes(int64(len(p)))
		c := int(binary.LittleEndian.Uint32(p))
		l := int(binary.LittleEndian.Uint32(p[4:]))
		if c != i || l != len(p) {
			b.FailNow()
		}
	}
	b.StopTimer()
}
