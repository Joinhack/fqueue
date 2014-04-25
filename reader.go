package fqueue

import (
	"errors"
	"io"
	"os"
)

var errNegativeRead = errors.New("fqueue: reader returned negative count from Read")

type Reader struct {
	fd *os.File
	*FQueue
}

func NewReader(path string, q *FQueue) (r *Reader, err error) {
	var fd *os.File
	fd, err = os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	if _, err = fd.Seek(int64(q.ReaderOffset), os.SEEK_SET); err != nil {
		return
	}

	if q == nil {
		panic("can't be nil")
	}
	r = &Reader{
		FQueue: q,
		fd:     fd,
	}
	err = nil
	return
}

func (b *Reader) remain() int {
	return b.WriterBottom - b.ReaderOffset
}

func (b *Reader) rolling() (err error) {
	_, err = b.fd.Seek(MetaSize, os.SEEK_SET)
	b.ReaderOffset = MetaSize
	return
}

func (b *Reader) Close() error {
	return b.fd.Close()
}

func (b *Reader) Read(p []byte) (n int, err error) {
	return b._read(p)
}

func (b *Reader) _read(p []byte) (n int, err error) {
	remain := b.remain()
	e := len(p)
	if remain == 0 {
		err = io.EOF
		return
	}
	if remain < e {
		e = remain
	}

	n, err = b.fd.Read(p[:e])
	return
}
