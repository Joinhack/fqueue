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
	MaxJournalFiles = 5
	JournalFileLimit = 4096 * K
	if jq, err = NewJournalQueue(fpath); err != nil {
		t.Error(err)
		t.FailNow()
	}
	var p []byte
	ptr := make([]byte, 256 + 8)
	for i := 0; i < 100000; i++ {
		l := rand.Intn(256) + 8
		p = ptr[:l]
		binary.LittleEndian.PutUint32(p, uint32(i))
		binary.LittleEndian.PutUint32(p[4:], uint32(l))
		if err = jq.Push(p); err != nil {
			t.Error(err)
			t.FailNow()
		}
	}
	for i := 0; i < 100000; i++ {
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
