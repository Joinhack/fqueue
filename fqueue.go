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

const (
	readTryTimes  = 3
	writeTryTimes = 3
)

var (
	NoSpace          = errors.New("no space error")
	MunMapErr        = errors.New("munmap error")
	QueueEmpty       = errors.New("queue is empty")
	InvalidMeta      = errors.New("invalid meta")
	MustBeFile       = errors.New("must be file")
	ReachMaxTryTimes = errors.New("reach max retry times for read/write")
	magic            = "JFQ"
)

type meta struct {
	WriterOffset int
	ReaderOffset int
	WriterBottom int
	Limit        int
	FSize        int
}

var metaSize uintptr = unsafe.Sizeof(meta{})
var magicLen int = len(magic)

var (
	FileLimit   = 1024 * 1024 * 50
	PrepareCall func(int)
)

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
	Close() error
}

type FQueue struct {
	*meta
	*Writer
	*Reader
	meta1       *meta
	lastFlushTime int64
	metaFd        *os.File
	metaPtr       []byte
	running       bool
	qMutex        *sync.Mutex
}

func (q *FQueue) GetMeta() *meta {
	return q.meta
}

func PrintMeta(m *meta) {
	println("File Contents:", m.FSize)
	println("Limit:", m.Limit)
	println("ReaderOffset:", m.ReaderOffset)
	println("WriterOffset:", m.WriterOffset)
	println("WriterBottom:", m.WriterBottom)
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

	if needSpace+int(q.FSize) > int(q.Limit-MetaSize) {
		return NoSpace
	}
	if (q.WriterOffset < q.WriterBottom) && (q.WriterOffset+needSpace >= q.ReaderOffset) {
		return NoSpace
	}

	if int(q.WriterOffset+needSpace) >= q.Limit {
		//origin: q.Limit-q.WriterBottom+needSpace+q.FSize < q.Limit-MetaSize
		if needSpace+q.FSize-q.WriterBottom < -MetaSize {
			err = q.Writer.rolling()
		} else {
			return NoSpace
		}
	}

	if err = binary.Write(q, binary.LittleEndian, uint16(plen)); err != nil {
		return err
	}
	if _, err = q.Write(p); err != nil {
		return err
	}

	q.FSize += (needSpace)
	q.WriterOffset += needSpace
	q.Writer.setBottom()
	q.dumpMeta(q.meta)
	return err
}

func (q *FQueue) prepareQueueFile(path string, limit int) {
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
			PrepareCall(i)
		}
	}
	fmt.Println("prepared queue file, used time:", (time.Now().UnixNano()-n.UnixNano())/1000000, "ms")
}

func (fq *FQueue) metaMapper(offset uintptr) *meta {
	h := (*struct {
		ptr  uintptr
		l, c int
	})(unsafe.Pointer(&fq.metaPtr))
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
			q.prepareQueueFile(path, fileLimit)
			q.metaFd, err = os.OpenFile(path, os.O_RDWR, 0644)
			if err != nil {
				return
			}
			q.metaPtr, err = mmap(q.metaFd.Fd(), 0, MetaSize, RDWR)
			if err != nil {
				return
			}

			q.meta = q.metaMapper(uintptr(magicLen))
			q.meta1 = q.metaMapper(uintptr(magicLen) + metaSize)
			q.Limit = fileLimit
			q.FSize = 0
			q.meta.ReaderOffset = MetaSize
			q.meta.WriterOffset = MetaSize
			q.meta.WriterBottom = q.meta.WriterOffset
			copy(q.metaPtr, []byte(magic))
			q.dumpMeta(q.meta)
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
	}

	if q.Reader, err = NewReader(path, q); err != nil {
		return
	}
	if q.Writer, err = NewWriter(path, q); err != nil {
		return
	}
	fq = q
	return
}

func (q *FQueue) Close() error {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()

	if err := q.Writer.Close(); err != nil {
		return err
	}
	if err := q.Reader.Close(); err != nil {
		return err
	}

	q.dumpMeta(q.meta)

	if len(q.metaPtr) > 0 {
		if err := unmap(q.metaPtr); err != nil {
			return err
		}
	}
	if err := q.metaFd.Close(); err != nil {
		return err
	}
	
	return nil
}

func (q *FQueue) loadMeta(path string) error {
	var err error
	q.metaFd, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	q.metaPtr, err = mmap(q.metaFd.Fd(), 0, MetaSize, RDWR)
	if err != nil {
		return err
	}
	if string(q.metaPtr[:magicLen]) != magic {
		return InvalidMeta
	}
	q.meta = q.metaMapper(uintptr(magicLen))
	q.meta1 = q.metaMapper(uintptr(magicLen) + metaSize)
	if *q.meta1 != *q.meta {
		*q.meta = *q.meta1 
	}
	return nil
}

func (q *FQueue) dumpMeta(meta *meta) error {
	var p = q.metaPtr
	p = p[magicLen:]
	*q.meta1 = *q.meta
	return nil
}

func (q *FQueue) Pop() (p []byte, err error) {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	var l uint16
	if q.FSize == 0 {
		err = QueueEmpty
		return
	}

	if q.ReaderOffset == q.WriterBottom && q.WriterOffset < q.WriterBottom {
		q.WriterBottom = q.WriterOffset
		if err = q.Reader.rolling(); err != nil {
			return
		}
	}

	var lbuf [2]byte
	var n, c, retry int
	c = 0

	for retry = readTryTimes; retry > 0 && c < len(lbuf); retry-- {
		if n, err = q.Read(lbuf[c:]); err != nil {
			return
		}
		c += n
	}
	if retry <= 0 {
		err = ReachMaxTryTimes
		return
	}
	l = binary.LittleEndian.Uint16(lbuf[:])
	p = make([]byte, l)
	c = 0
	n = 0
	for retry = readTryTimes; retry > 0 && c < int(l); retry-- {
		if n, err = q.Read(p[c:]); err != nil {
			return
		}
		c += n
	}
	if retry <= 0 {
		err = ReachMaxTryTimes
		return
	}

	q.ReaderOffset += int(2 + l)
	q.FSize -= int(2 + l)
	q.dumpMeta(q.meta)
	return
}
