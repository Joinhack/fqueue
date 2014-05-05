package fqueue

import (
	"os"
	"syscall"
	"unsafe"
)

/*
#include <sys/mman.h>
*/
import "C"

type Writer struct {
	p      []byte
	offset int64
	ptr    []byte
	fd     *os.File
	*FQueue
}

func (b *Writer) rolling() error {
	b.unmapper()
	b.WriterOffset = MetaSize
	b.offset = int64(b.WriterOffset)
	b.p = b.p[:0]
	return nil
}

func (b *Writer) setBottom() {
	if b.ReaderOffset < b.WriterOffset && b.WriterOffset > b.WriterBottom && b.FSize < b.Limit {
		b.WriterBottom = b.WriterOffset
	}
}

func (b *Writer) Close() error {
	if err := b.unmapper(); err != nil {
		return err
	}
	return b.fd.Close()
}

func (b *Writer) unmapper() error {
	if len(b.ptr) > 0 {
		// I don't know why it always return "invalid argument"
		// if err := syscall.Munmap(b.ptr); err != nil {
		// 	return err
		// }
		slice := (*struct {
			p    uintptr
			l, c int
		})(unsafe.Pointer(&b.ptr))
		err := C.munmap(unsafe.Pointer(slice.p), C.size_t(slice.l))
		if err != 0 {
			panic("munmapper error")
		}
	}
	return nil
}

func (b *Writer) mapper() (err error) {
	if err = b.unmapper(); err != nil {
		return
	}
	b.ptr, err = syscall.Mmap(int(b.fd.Fd()), b.offset, PageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	b.p = b.ptr
	b.offset += PageSize
	return
}

func (b *Writer) Write(p []byte) (n int, err error) {
	c := 0
	lp := len(p)
	var retry int
	for retry = writeTryTimes; retry > 0 && c < lp; retry-- {
		if len(b.p) == 0 {
			err = b.mapper()
			if err != nil {
				return
			}
		}
		n = copy(b.p, p[c:])
		b.p = b.p[n:]
		c += n
	}
	if retry <= 0 {
		err = ReachMaxTryTimes
		return
	}
	return
}

func NewWriter(path string, q *FQueue) (w *Writer, err error) {
	var fd *os.File
	fd, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return
	}
	w = &Writer{
		FQueue: q,
		fd:     fd,
	}
	//set the mapper offset
	if w.WriterOffset%PageSize == 0 {
		w.offset = int64(w.WriterOffset)
	} else {
		offset := (w.WriterOffset/PageSize + 1) * PageSize
		if offset > q.Limit {
			offset = q.Limit
		}
		w.offset = int64(offset)
	}
	err = nil
	return
}
