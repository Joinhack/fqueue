package fqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"
)

var (
	NoSpace     = errors.New("no space error")
	QueueEmpty  = errors.New("queue is empty")
	InvalidMeta = errors.New("invalid meta")
	MustBeFile  = errors.New("must be file")
	magic       = "JFQ"
)

type meta struct {
	WriterOffset int
	ReaderOffset int
	WriterBottom int
	Limit        int
	Contents     int
}

var metaSize uintptr = unsafe.Sizeof(meta{})
var magicLen int = len(magic)

var (
	FileLimit                  = 1024 * 1024 * 50
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
			//err = q.Writer.rolling()
			q.WriterOffset = MetaSize
		} else {
			return NoSpace
		}
	}
	binary.LittleEndian.PutUint16(q.ptr[q.WriterOffset:], uint16(plen))
	copy(q.ptr[q.WriterOffset+2:], p)
	q.Contents += (needSpace)
	q.WriterOffset += needSpace
	//q.Writer.setBottom()
	if q.ReaderOffset < q.WriterOffset &&
		q.WriterOffset > q.WriterBottom &&
		q.Contents < q.Limit {
		q.WriterBottom = q.WriterOffset
	}
	q.mergeMeta(q.meta)
	return err
}

func (fq *FQueue) metaMapper(offset uintptr) *meta {
	h := (*struct {
		ptr  uintptr
		l, c int
	})(unsafe.Pointer(&fq.ptr))
	return (*meta)(unsafe.Pointer(h.ptr + offset))
}

func NewFQueue(path string) (fq *FQueue, err error) {
	limit := FileLimit
	if limit < PageSize {
		limit = PageSize
	}

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

			q.meta = q.metaMapper(uintptr(magicLen))
			q.meta1 = q.metaMapper(uintptr(magicLen) + metaSize)
			q.Limit = fileLimit
			q.Contents = 0
			q.meta.ReaderOffset = MetaSize
			q.meta.WriterOffset = MetaSize
			q.meta.WriterBottom = q.meta.WriterOffset
			q.mergeMeta(q.meta)
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
		if err = q.loadMeta(path, st.Size()); err != nil {
			return
		}
	}
	fq = q
	return
}

func (q *FQueue) Close() error {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()

	q.mergeMeta(q.meta)

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

func (q *FQueue) loadMeta(path string, size int64) error {
	var err error
	q.fd, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	q.ptr, err = mmap(q.fd.Fd(), 0, int(size), RDWR)
	if err != nil {
		return err
	}
	if string(q.ptr[:magicLen]) != magic {
		return InvalidMeta
	}
	q.meta = q.metaMapper(uintptr(magicLen))
	q.meta1 = q.metaMapper(uintptr(magicLen) + metaSize)
	if *q.meta1 != *q.meta {
		*q.meta = *q.meta1
	}
	return nil
}

func (q *FQueue) mergeMeta(meta *meta) error {
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

	q.ReaderOffset += int(2 + l)
	q.Contents -= int(2 + l)
	q.mergeMeta(q.meta)
	return
}
