package fqueue

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)


func BenchmarkJournalQueuePush(b *testing.B) {
	b.N = 1000000
	var err error
	var p []byte
	var fq Queue
	fpath = filepath.Join(os.TempDir(), "journal1")
	os.RemoveAll(fpath)
	MaxJournalFiles = 10
	JournalFileLimit = 20 * M
	if err := PrepareAllJournalFiles(fpath); err != nil {
		b.Error(err)
		b.FailNow()
	}
	if fq, err = NewJournalQueue(fpath); err != nil {
		panic(err)
	}
	defer fq.Close()
	ptr := make([]byte, 256 + 8)
	for i := 0; i < 256 + 8; i++ {
		ptr[i] = 'T'
	}
	b.ResetTimer()
	b.StartTimer()
	var total int64 = 0
	for i := 0; i < b.N; i++ {
		l := rand.Intn(256) + 8
		p = ptr[:l]
		total += int64(len(p))
		binary.LittleEndian.PutUint32(p, uint32(i))
		binary.LittleEndian.PutUint32(p[4:], uint32(l))

		if err = fq.Push(p); err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
	b.StopTimer()
	
}

func BenchmarkJournalQueuePop(b *testing.B) {
	b.N = 1000000
	var err error
	var p []byte
	var fq Queue
	if fq, err = NewJournalQueue(fpath); err != nil {
		panic(err)
	}
	defer fq.Close()
	defer os.RemoveAll(fpath)
	b.ResetTimer()
	b.StartTimer()
	var total int64 = 0
	for i := 0; i < b.N; i++ {
		if p, err = fq.Pop(); err != nil {
			b.FailNow()
			return
		}
		total += int64(len(p))
		c := int(binary.LittleEndian.Uint32(p))
		l := int(binary.LittleEndian.Uint32(p[4:]))
		if c != i || l != len(p) {
			b.FailNow()
		}
	}
	b.StopTimer()
}
