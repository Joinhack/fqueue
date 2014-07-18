package fqueue

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

var fq *FQueue

func init() {
	var err error
	fpath := filepath.Join(os.TempDir(), "fq1_benchmark.data")
	os.Remove(fpath)
	FileLimit = 1000000 * (256 + 8)
	if fq, err = NewFQueue(fpath); err != nil {
		panic(err)
	}
}

func BenchmarkPush(b *testing.B) {
	b.N = 1000000
	var err error
	var p []byte
	for i := 0; i < b.N; i++ {
		l := rand.Intn(256) + 8
		p = make([]byte, l)
		b.SetBytes(int64(l))
		for j := 8; j < l; j++ {
			p[j] = byte(l%255)
		}
		binary.LittleEndian.PutUint32(p, uint32(i))
		binary.LittleEndian.PutUint32(p[4:], uint32(l))

		if err = fq.Push(p); err != nil {
			b.Error(err)
			b.FailNow()
		}
	}
}

func BenchmarkPop(b *testing.B) {
	b.N = 1000000
	var err error
	var p []byte
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
}
