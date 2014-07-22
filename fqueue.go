package fqueue

import (
	"encoding/binary"
	"hash/crc32"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
	"path/filepath"
	"unsafe"
)

var (
	NoSpace     = errors.New("no space error")
	QueueEmpty  = errors.New("queue is empty")
	InvalidMeta = errors.New("invalid meta")
	MustBeFile  = errors.New("must be file")
)

var (
	magic                  = "JFQ"
	metaStructSize uintptr = unsafe.Sizeof(meta{})
	magicLen       int     = len(magic)

	FileLimit = 1024 * 1024 * 50
	ChunkSize = 1024 * 1024 * 512

	PrepareCall func(int, int) = func(limit, now int) {
		if now == 4096 {
			fmt.Print(".")
		} else if now%(1024*1024*5) == 0 {
			fmt.Print(".")
		}
		if now == limit {
			fmt.Println("100%")
		}
	}
)

const (
	WRMask = 0x1
	RDMask = 0x2
)

type meta struct {
	Mask         byte
	Idx          byte
	Group        uint32
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

type ChunksQueue struct {
	chunks    []*FQueue
	readerIdx int
	writerIdx int
}

func (q *FQueue) GetMeta() *meta {
	return q.meta
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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	empty := make([]byte, 4096)
	n := time.Now()
	fmt.Println("prepared queue file")
	for i := 0; i < limit; {
		file.Write(empty)
		i += len(empty)
		//file queue prepared callback.
		if PrepareCall != nil {
			PrepareCall(limit, i)
		}
	}
	fmt.Println("prepared queue file, used time:", (time.Now().UnixNano()-n.UnixNano())/1000000, "ms")
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
	if int(q.WriterOffset+needSpace) >= q.Limit {
		//origin: q.Limit-q.WriterBottom+needSpace+q.Contents < q.Limit-MetaSize
		if needSpace+q.Contents-q.WriterBottom < -MetaSize {
			//rolling
			q.WriterOffset = MetaSize
		} else {
			return NoSpace
		}
	}
	binary.LittleEndian.PutUint16(q.ptr[q.WriterOffset:], uint16(plen))
	copy(q.ptr[q.WriterOffset+2:], p)
	q.Mask |= WRMask
	q.Contents += (needSpace)
	q.WriterOffset += needSpace
	//set bottom.
	if q.ReaderOffset < q.WriterOffset &&
		q.WriterOffset > q.WriterBottom &&
		q.Contents < q.Limit {
		q.WriterBottom = q.WriterOffset
	}
	q.mergeMeta()
	return err
}

func metaMapper(offset uintptr, ptr []byte) *meta {
	h := (*struct {
		ptr  uintptr
		l, c int
	})(unsafe.Pointer(&ptr))
	return (*meta)(unsafe.Pointer(h.ptr + offset))
}

func newFQueue(path string, limit int, idx byte, group uint32) (fq *FQueue, err error) {
	//PageSize * n should be limit
	if limit%PageSize != 0 {
		limit = (limit/PageSize)*PageSize + PageSize
	}

	fileLimit := limit + MetaSize

	q := &FQueue{
		qMutex:  &sync.Mutex{},
		running: true,
	}

	var st os.FileInfo
	if st, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			prepareQueueFile(path, fileLimit)
			q.fd, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
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
			q.meta.Idx = idx
			q.meta.ReaderOffset = MetaSize
			q.meta.WriterOffset = MetaSize
			q.meta.WriterBottom = q.meta.WriterOffset
			q.meta.Group = group
			q.mergeMeta()
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
			err = InvalidMeta
			return
		}
		if err = q.loadMeta(path); err != nil {
			return
		}
		if crc32.ChecksumIEEE([]byte(path)) != q.meta.Group {
			err = InvalidMeta
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
	return newFQueue(absPath, FileLimit, 0, crc32.ChecksumIEEE([]byte(absPath)))
}

func (q *FQueue) Close() error {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()

	q.mergeMeta()

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

func (q *FQueue) loadMeta(path string) error {
	var err error
	var n int
	q.fd, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	metaSli := make([]byte, MetaSize)
	if n, err = q.fd.Read(metaSli); err != nil {
		return err
	}
	if n != MetaSize {
		return errors.New(fmt.Sprintf("hope read %d bytes, but read %d bytes.", MetaSize, n))
	}
	readonlyMeta := metaMapper(uintptr(magicLen), metaSli)

	if string(metaSli[:magicLen]) != magic {
		return InvalidMeta
	}

	if _, err = q.fd.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	q.ptr, err = mmap(q.fd.Fd(), 0, readonlyMeta.Limit, RDWR)
	if err != nil {
		return err
	}
	if string(q.ptr[:magicLen]) != magic {
		return InvalidMeta
	}
	q.meta = metaMapper(uintptr(magicLen), q.ptr)
	q.meta1 = metaMapper(uintptr(magicLen)+metaStructSize, q.ptr)
	if *q.meta1 != *q.meta {
		*q.meta = *q.meta1
	}
	return nil
}

func (q *FQueue) mergeMeta() error {
	*q.meta1 = *q.meta
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

	if q.ReaderOffset == q.WriterBottom &&
		q.WriterOffset < q.WriterBottom {
		q.WriterBottom = q.WriterOffset
		q.ReaderOffset = MetaSize
	}

	l = binary.LittleEndian.Uint16(q.ptr[q.ReaderOffset:])
	p = make([]byte, l)
	copy(p, q.ptr[q.ReaderOffset+2:])
	q.Mask |= RDMask
	q.ReaderOffset += int(2 + l)
	q.Contents -= int(2 + l)
	q.mergeMeta()
	return
}
