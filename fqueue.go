package fqueue

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unsafe"
)

const (
	MetaMask   = 0x1 //if set, the meta is already.
	MetaUnMask = ^MetaMask & 0xff
)

var (
	magic                  = "JFQ"
	metaStructSize uintptr = unsafe.Sizeof(meta{})
	magicLen       int     = len(magic)

	QueueLimit = 1024 * 1024 * 50
)

type meta struct {
	Mask         byte
	Idx          int
	WriterOffset int
	ReaderOffset int
	WriterBottom int
	Limit        int
	Contents     int
}

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
	Close() error
}

type FQueue struct {
	*meta
	meta1   *meta
	fd      *os.File
	ptr     []byte
	running bool
	qMutex  *sync.Mutex
}

func PrintMeta(m *meta) {
	fmt.Println("File Contents:", m.Contents)
	fmt.Println("Limit:", m.Limit)
	fmt.Println("ReaderOffset:", m.ReaderOffset)
	fmt.Println("WriterOffset:", m.WriterOffset)
	fmt.Println("WriterOffset:", m.WriterOffset)
	fmt.Println("WriterBottom:", m.WriterBottom)
}

func (q *FQueue) PrintMeta() {
	fmt.Println("--------meta--------")
	PrintMeta(q.meta)
	fmt.Println("--------meta1--------")
	PrintMeta(q.meta1)
}

func prepareQueueFile(path string, limit int) {
	if err := os.Truncate(path, int64(limit)); err != nil {
		panic(err)
	}
}

func (q *FQueue) Push(p []byte) error {
	var plen = len(p)
	if plen == 0 {
		return nil
	}
	var err error
	q.qMutex.Lock()
	defer q.qMutex.Unlock()

	var needSpace = plen + 2

	if needSpace+int(q.Contents) > int(q.Limit-MetaSize) {
		return NoSpace
	}
	if (q.WriterOffset < q.WriterBottom) &&
		(q.WriterOffset+needSpace >= q.ReaderOffset) {
		return NoSpace
	}
	q.Mask &= MetaUnMask
	if int(q.WriterOffset+needSpace) >= q.Limit {
		//if add the item will to exceed the bottom of file(the limit) and
		//the free space is more than need space, rolling.
		if q.ReaderOffset-MetaSize > needSpace {
			//rolling
			q.WriterOffset = MetaSize
		} else {
			//set mask
			q.Mask |= MetaMask
			return NoSpace
		}
	}
	binary.LittleEndian.PutUint16(q.ptr[q.WriterOffset:], uint16(plen))
	copy(q.ptr[q.WriterOffset+2:], p)
	//set contents
	q.Contents += needSpace
	//set writer offset
	q.WriterOffset += needSpace
	//set bottom.
	if q.ReaderOffset < q.WriterOffset &&
		q.WriterOffset > q.WriterBottom &&
		q.Contents < q.Limit {
		q.WriterBottom = q.WriterOffset
	}

	q.Mask |= MetaMask
	//merge meta
	*q.meta1 = *q.meta
	return err
}

func metaMapper(offset uintptr, ptr []byte) *meta {
	h := (*struct {
		ptr  uintptr
		l, c int
	})(unsafe.Pointer(&ptr))
	return (*meta)(unsafe.Pointer(h.ptr + offset))
}

func getAlign(limit int) int {
	if limit%PageSize != 0 {
		limit = (limit/PageSize)*PageSize + PageSize
	}
	return limit + MetaSize
}

func newFQueue(path string, fileLimit int) (fq *FQueue, err error) {

	q := &FQueue{
		qMutex:  &sync.Mutex{},
		running: true,
	}
	defer func() {
		if err != nil && q.fd != nil {
			q.fd.Close()
		}
	}()
	var st os.FileInfo
	if st, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			q.fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return
			}
			if err = q.fd.Truncate(int64(fileLimit)); err != nil {
				return
			}
			q.ptr, err = mmap(q.fd.Fd(), 0, fileLimit, RDWR)
			if err != nil {
				return
			}
			q.meta = metaMapper(uintptr(magicLen), q.ptr)
			q.meta1 = metaMapper(uintptr(magicLen)+metaStructSize, q.ptr)
			q.Limit = fileLimit
			q.Contents = 0
			q.meta.ReaderOffset = MetaSize
			q.meta.WriterOffset = MetaSize
			q.meta.WriterBottom = q.meta.WriterOffset
			//merge meta
			*q.meta1 = *q.meta
			copy(q.ptr, []byte(magic))
			if err != nil {
				return
			}
		} else {
			return
		}
	} else {
		if st.IsDir() {
			err = MustBeFile
			return
		}
		if st.Size() < MetaSize {
			err = InvaildMeta
			return
		}
		if err = q.loadMeta(path); err != nil {
			return
		}
	}
	fq = q
	return
}

func NewFQueue(path string) (fq Queue, err error) {
	var absPath string
	if absPath, err = filepath.Abs(path); err != nil {
		return
	}
	absPath = filepath.ToSlash(absPath)
	return newFQueue(absPath, getAlign(QueueLimit))
}

func (q *FQueue) Close() error {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()

	//merge meta
	*q.meta1 = *q.meta

	if len(q.ptr) > 0 {
		if err := unmap(q.ptr); err != nil {
			return err
		}
	}
	q.fd.Sync()
	if err := q.fd.Close(); err != nil {
		return err
	}
	return nil
}

func (q *FQueue) loadMeta(path string) (err error) {
	var n int
	q.fd, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			q.fd.Close()
			q.fd = nil
		}
	}()
	metaSli := make([]byte, MetaSize)
	if n, err = q.fd.Read(metaSli); err != nil {
		return
	}
	if n != MetaSize {
		err = InvaildReadn
		return
	}
	//find the mmap size info in the meta.
	readonlyMeta := metaMapper(uintptr(magicLen), metaSli)

	if string(metaSli[:magicLen]) != magic {
		err = InvaildMeta
		return
	}

	//reset file.
	if _, err = q.fd.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	if err = q.fd.Truncate(int64(readonlyMeta.Limit)); err != nil {
		return
	}
	q.ptr, err = mmap(q.fd.Fd(), 0, readonlyMeta.Limit, RDWR)
	if err != nil {
		return
	}
	if string(q.ptr[:magicLen]) != magic {
		return InvaildMeta
	}
	q.meta = metaMapper(uintptr(magicLen), q.ptr)
	q.meta1 = metaMapper(uintptr(magicLen)+metaStructSize, q.ptr)

	//if meta is not ready, use the backup meta.
	if q.Mask&MetaMask == 0 {
		*q.meta = *q.meta1
	}
	return nil
}

func (q *FQueue) Pop() (p []byte, err error) {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	var l uint16
	if q.Contents == 0 {
		err = QueueEmpty
		return
	}

	q.Mask &= MetaUnMask

	//when the reader offset reach the bottom, rolling the reader offset and
	//set the bottom to the writer offset.
	if q.ReaderOffset == q.WriterBottom &&
		q.WriterOffset < q.WriterBottom {
		q.WriterBottom = q.WriterOffset
		q.ReaderOffset = MetaSize
	}

	l = binary.LittleEndian.Uint16(q.ptr[q.ReaderOffset:])
	p = make([]byte, l)
	copy(p, q.ptr[q.ReaderOffset+2:])

	//set the reader offset
	q.ReaderOffset += int(2 + l)

	//calc the content in fqueue
	q.Contents -= int(2 + l)
	q.Mask |= MetaMask

	//merge meta
	*q.meta1 = *q.meta
	return
}
