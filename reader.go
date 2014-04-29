package fqueue

import (
	"errors"
	//"io"
	"os"
	"reflect"
	"unsafe"
)

/*
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
*/
import "C"

var errNegativeRead = errors.New("fqueue: reader returned negative count from Read")

type Reader struct {
	fd *os.File
	*FQueue
	p      []byte
	offset int64
	ptr    unsafe.Pointer
}

func NewReader(path string, q *FQueue) (r *Reader, err error) {
	var fd *os.File
	fd, err = os.OpenFile(path, os.O_RDWR, 0644)
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
	if b.ptr != nil {
		if c := C.munmap(b.ptr, PageSize); c != 0 {
			return MunMapErr
		}
		b.ptr = nil
	}
	return nil
}

func (b *Reader) mapper() (err error) {
	if err = b.unmapper(); err != nil {
		return
	}
	b.ptr, err = C.mmap(nil, C.size_t(PageSize), C.PROT_WRITE, C.MAP_SHARED, C.int(b.fd.Fd()), C.off_t(b.offset))
	if err != nil {
		panic(err)
	}
	var sliceH reflect.SliceHeader
	sliceH.Cap = PageSize
	sliceH.Len = PageSize
	sliceH.Data = uintptr(b.ptr)
	b.p = *(*[]byte)(unsafe.Pointer(&sliceH))
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
	return b._read(p)
}

func (b *Reader) _read(p []byte) (n int, err error) {
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
