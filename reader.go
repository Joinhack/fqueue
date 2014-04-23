package fqueue

import (
	"errors"
	"io"
	"os"
)

var errNegativeRead = errors.New("fqueue: reader returned negative count from Read")

type Reader struct {
	buf  []byte
	fd   *os.File
	w, r int
	err  error
	*FQueue
}

func (b *Reader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

func (b *Reader) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return 0, b.readErr()
	}
	if b.w == b.r {
		if b.err != nil {
			return 0, b.readErr()
		}
		if len(p) >= len(b.buf) {
			// Large read, empty buffer.
			// Read directly into p to avoid copy.
			n, b.err = b.fileRead(p)
			return n, b.readErr()
		}
		b.fill()
		if b.w == b.r {
			return 0, b.readErr()
		}
	}

	if n > b.w-b.r {
		n = b.w - b.r
	}
	copy(p[0:n], b.buf[b.r:])
	b.r += n
	return n, nil
}

func NewReader(path string, q *FQueue) (r *Reader, err error) {
	var fd *os.File
	fd, err = os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	if _, err = fd.Seek(int64(q.ReaderPtr), os.SEEK_SET); err != nil {
		return
	}

	if q == nil {
		panic("can't be nil")
	}
	r = &Reader{
		FQueue: q,
		fd:     fd,
		buf:    make([]byte, 4096),
	}
	err = nil
	return
}

func (b *Reader) remain() int {
	return b.WriterBottom - b.ReaderOffset
}

func (b *Reader) rolling() (err error) {
	b.ReaderOffset = 1024
	_, err = b.fd.Seek(1024, os.SEEK_SET)
	return
}

func (b *Reader) fileRead(p []byte) (n int, err error) {
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
	b.ReaderOffset += n
	b.Free += n
	if b.ReaderOffset == b.WriterBottom {
		b.rolling()
	}
	return
}

func (b *Reader) fill() {
	// Slide existing data to beginning.
	if b.r > 0 {
		copy(b.buf, b.buf[b.r:b.w])
		b.w -= b.r
		b.r = 0
	}
	var n int
	var err error
	n, err = b.fileRead(b.buf[b.w:])
	// Read new data.
	if n < 0 {
		panic(errNegativeRead)
	}
	b.w += n
	if err != nil {
		b.err = err
	}
}
