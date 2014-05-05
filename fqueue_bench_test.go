package fqueue

import (
	"encoding/binary"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	b.N = 1000000
	var err error
	var d []byte
	var fq *FQueue
	FileLimit = 1000000 * (256 + 8)
	os.Remove("/tmp/fq_bench1.data")
	if fq, err = NewFQueue("/tmp/fq_bench1.data"); err != nil {
		panic(err)
	}
	var total = 0
	for i := 0; i < b.N; i++ {
		l := rand.Intn(256) + 8
		d = make([]byte, l)
		total += l
		binary.LittleEndian.PutUint32(d, uint32(i))
		binary.LittleEndian.PutUint32(d[4:], uint32(l))

		if err = fq.Push(d); err != nil {
			b.Error(err)
			println(i)
			b.FailNow()
		}
	}
	fq.Close()
}

func BenchmarkRead(b *testing.B) {
	b.N = 1000000
	var fq *FQueue
	var err error
	if fq, err = NewFQueue("/tmp/fq_bench1.data"); err != nil {
		panic(err)
	}
	if fq == nil {
		b.FailNow()
		return
	}

	var total = 0
	var p []byte

	for i := 0; i < b.N; i++ {
		if p, err = fq.Pop(); err != nil {
			println("0000000", i)
			b.FailNow()
			return
		}

		c := int(binary.LittleEndian.Uint32(p))
		l := int(binary.LittleEndian.Uint32(p[4:]))
		total += l
		if c != i || l != len(p) {
			b.FailNow()
		}
	}
	fq.Close()
	os.Remove("/tmp/fq_bench1.data")
}
