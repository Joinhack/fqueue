package fqueue

import (
	"errors"
	"io"
	"os"
)

var errNegativeRead = errors.New("fqueue: reader returned negative count from Read")

type Reader struct {
	buf []byte
	fd  *os.File
	p   []byte
	err error
	*FQueue
}

func (b *Reader) readErr() error {
	err := b.err
	b.err = nil
	return err
}

// func (b *Reader) Read(p []byte) (n int, err error) {
// 	n = len(p)
// 	if n == 0 {
// 		return 0, b.readErr()
// 	}
// 	if b.w == b.r {
// 		if b.err != nil {
// 			return 0, b.readErr()
// 		}
// 		if len(p) >= len(b.buf) {
// 			// Large read, empty buffer.
// 			// Read directly into p to avoid copy.
// 			n, b.err = b.fdread(p)
// 			return n, b.readErr()
// 		}
// 		b.fill()
// 		if b.w == b.r {
// 			return 0, b.readErr()
// 		}
// 	}

// 	if n > b.w-b.r {
// 		n = b.w - b.r
// 	}
// 	copy(p[0:n], b.buf[b.r:])
// 	b.r += n
// 	return n, nil
// }

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
		buf:    make([]byte, 4096),
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
	b.p = b.p[0:0]
	return
}

func (b *Reader) Close() error {
	return b.fd.Close()
}

func (b *Reader) read1(p []byte) (n int, err error) {
	if len(b.p) == 0 {
		n, err = b.fdread(b.buf)
		if err != nil {
			b.err = err
			return
		}
		if n == 0 {
			err = io.EOF
			return
		}
		b.p = b.buf[:n]
	}

	copy(p, b.p)
	b.p = b.p[len(p):]
	n = len(p)
	return
}

func (b *Reader) Read(p []byte) (n int, err error) {
	return b.fd.Read(p)
}

func (b *Reader) fdread(p []byte) (n int, err error) {
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

// func (b *Reader) fill() {
// 	// Slide existing data to beginning.
// 	//println("filling before", b.w, b.r)
// 	if b.r > 0 {
// 		copy(b.buf, b.buf[b.r:b.w])
// 		b.w -= b.r
// 		b.r = 0
// 	}
// 	var n int
// 	var err error

// 	n, err = b.fdread(b.buf[b.w:])
// 	// Read new data.
// 	if n < 0 {
// 		panic(errNegativeRead)
// 	}
// 	b.w += n
// 	if err != nil {
// 		b.err = err
// 	}
// }
