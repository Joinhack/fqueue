package fqueue

import (
	"os"
)

type Reader struct {
	fd *os.File
	*FQueue
	p      []byte
	offset int64
	ptr    []byte
}

func NewReader(path string, q *FQueue) (r *Reader, err error) {
	var fd *os.File
	fd, err = os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return
	}

	if q == nil {
		panic("can't be nil")
	}
	r = &Reader{
		FQueue: q,
		fd:     fd,
		offset: int64(q.ReaderOffset),
	}
	//set the mapper offset
	if r.ReaderOffset%PageSize == 0 {
		r.offset = int64(r.ReaderOffset)
	} else {
		offset := (r.ReaderOffset/PageSize + 1) * PageSize
		if offset > q.Limit {
			offset = q.Limit
		}
		r.offset = int64(offset)
	}
	err = nil
	return
}

func (b *Reader) SetReaderOffset(o int) {
	b.ReaderOffset = o
	b.offset = int64(b.ReaderOffset)
}

func (b *Reader) remain() int {
	return b.WriterBottom - b.ReaderOffset
}

func (b *Reader) rolling() (err error) {
	b.ReaderOffset = MetaSize
	b.offset = int64(b.ReaderOffset)
	b.p = b.p[:0]
	return
}

func (b *Reader) unmapper() error {
	if len(b.ptr) > 0 {
		return unmap(b.ptr)
	}
	return nil
}

func (b *Reader) mapper() (err error) {
	if err = b.unmapper(); err != nil {
		return
	}
	b.ptr, err = mmap(b.fd.Fd(), b.offset, PageSize, RDONLY)
	if err != nil {
		return err
	}
	b.p = b.ptr
	b.offset += PageSize
	return
}

func (b *Reader) Close() error {
	if err := b.unmapper(); err != nil {
		return err
	}
	return b.fd.Close()
}

func (b *Reader) Read(p []byte) (n int, err error) {
	remain := b.remain()
	if remain == 0 {
		n = 0
		return
	}

	if len(b.p) == 0 {
		if err = b.mapper(); err != nil {
			return
		}
	}
	e := len(b.p)
	if remain < e {
		e = remain
	}
	n = copy(p, b.p[:e])
	b.p = b.p[n:]
	return
}
