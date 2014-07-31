package fqueue

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestPushPop(t *testing.T) {
	var fpath = filepath.Join(os.TempDir(), "journalQueue1")
	var jq *JournalQueue
	var err error
	MaxJournalFiles = 10
	JournalFileLimit = 2 * M
	os.RemoveAll(fpath)
	defer os.RemoveAll(fpath)
	if jq, err = NewJournalQueue(fpath); err != nil {
		t.Error(err)
		t.FailNow()
	}
	var p []byte
	ptr := make([]byte, 256 + 8)
	N := 100000
	for i := 0; i < N; i++ {
		l := rand.Intn(256) + 8
		p = ptr[:l]
		binary.LittleEndian.PutUint32(p, uint32(i))
		binary.LittleEndian.PutUint32(p[4:], uint32(l))
		if err = jq.Push(p); err != nil {
			t.Error(err)
			t.FailNow()
		}
	}
	for i := 0; i < N; i++ {
		if p, err = jq.Pop(); err != nil {
			if err == QueueEmpty {
				break
			}
		}
		c := int(binary.LittleEndian.Uint32(p))
		l := int(binary.LittleEndian.Uint32(p[4:]))
		if c != i || l != len(p) {
			t.FailNow()
		}
	}
	jq.Close()

}
