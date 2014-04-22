package fqueue

import (
	"io"
	"os"
)

type Writer struct {
	offset int
	buf    []byte
	n      int
	fd     *os.File
	err    error
	q      *FQueue
}

func (b *Writer) Flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	n, err := b.fd.Write(b.buf[0:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}

	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}
	b.offset += b.n
	b.q.fspace_free -= b.n
	b.n = 0
	return nil
}

func NewWriter(path string, q *FQueue) (w *Writer, err error) {
	var fd *os.File
	fd, err = os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	w = &Writer{
		q:   q,
		fd:  fd,
		buf: make([]byte, 4096),
	}
	err = nil
	return
}

func (b *Writer) Available() int { return len(b.buf) - b.n }

func (b *Writer) Write(bs []byte) (nn int, err error) {
	for len(bs) > b.Available() && b.err == nil {
		var n int
		if b.n == 0 {
			n, b.err = b.fd.Write(bs)
		} else {
			n = copy(b.buf[b.n:], bs)
			b.n += n
			b.Flush()
		}
		nn += n
		bs = bs[n:]
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], bs)
	b.n += n
	nn += n
	return nn, nil
}
