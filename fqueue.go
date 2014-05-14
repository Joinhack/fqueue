package fqueue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	PageSize      = 4096
	MetaSize      = 4096
	readTryTimes  = 3
	writeTryTimes = 3
)

var (
	DefaultDumpFlag byte = 0
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
	DumpFlag     byte //0 immediately, 1 dump meta every second
	WriterOffset int
	ReaderOffset int
	WriterBottom int
	Limit        int
	FSize        int
}

var (
	FileLimit   = 1024 * 1024 * 1024
	PrepareCall func(int)
)

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
	Close() error
}

type FQueue struct {
	meta
	*Writer
	*Reader
	lastFlushTime int64
	metaFd        *os.File
	metaPtr       []byte
	running       bool
	wg            *sync.WaitGroup
	qMutex        *sync.Mutex
}

func (q *FQueue) getMeta() *meta {
	return &q.meta
}

func (q *FQueue) printMeta() {
	m := q.getMeta()
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

	if needSpace+q.FSize > q.Limit-MetaSize {
		return NoSpace
	}
	if (q.WriterOffset < q.WriterBottom) && (q.WriterOffset+needSpace >= q.ReaderOffset) {
		return NoSpace
	}

	if q.WriterOffset+needSpace >= q.Limit {
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
	if q.DumpFlag == 0 {
		q.dumpMeta(&q.meta)
	}
	return err
}

func (q *FQueue) prepareQueueFile(path string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	empty := make([]byte, 4096)
	n := time.Now()
	fmt.Println("prepared queue file")
	for i := 0; i < q.Limit; {

		file.Write(empty)
		i += len(empty)
		//file queue prepared callback.
		if PrepareCall != nil {
			PrepareCall(i)
		}
	}
	fmt.Println("prepared queue file, used time:", (time.Now().UnixNano()-n.UnixNano())/1000000, "ms")
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
		wg:      &sync.WaitGroup{},
		running: true,
	}
	q.Limit = fileLimit
	q.FSize = 0
	q.DumpFlag = DefaultDumpFlag
	var st os.FileInfo
	if st, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			q.prepareQueueFile(path)
			q.metaFd, err = os.OpenFile(path, os.O_RDWR, 0644)
			if err != nil {
				return
			}
			q.metaPtr, err = syscall.Mmap(int(q.metaFd.Fd()), 0, MetaSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
			if err != nil {
				return
			}
			q.meta.ReaderOffset = MetaSize
			q.meta.WriterOffset = MetaSize
			q.meta.WriterBottom = q.meta.WriterOffset
			q.dumpMeta(&q.meta)
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
	if fq.DumpFlag == 1 {
		fq.wg.Add(1)
		go fq.metaTask()
	}
	return
}

func (q *FQueue) metaTask() {
	for q.running {
		select {
		case <-time.After(1 * time.Second):
		}
		q.qMutex.Lock()
		var meta meta
		meta = q.meta
		q.qMutex.Unlock()
		q.dumpMeta(&meta)
	}
	q.wg.Done()
}

func (q *FQueue) Close() error {
	if q.DumpFlag == 1 {
		q.running = false
		q.wg.Wait()
	}
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	q.dumpMeta(&q.meta)
	if len(q.metaPtr) > 0 {
		_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(unsafe.Pointer(&q.metaPtr[0])), uintptr(len(q.metaPtr)), 0)
		if errno != 0 {
			return syscall.Errno(errno)
		}
	}
	if err := q.metaFd.Close(); err != nil {
		return err
	}
	if err := q.Writer.Close(); err != nil {
		return err
	}
	if err := q.Reader.Close(); err != nil {
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
	q.metaPtr, err = syscall.Mmap(int(q.metaFd.Fd()), 0, MetaSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	p := q.metaPtr
	if string(p[:len(magic)]) != magic {
		return InvalidMeta
	}
	p = p[len(magic):]
	q.meta.DumpFlag = p[0]
	p = p[1:]
	q.meta.FSize = int(binary.LittleEndian.Uint64(p))
	p = p[8:]
	q.meta.Limit = int(binary.LittleEndian.Uint64(p))
	p = p[8:]
	q.meta.WriterBottom = int(binary.LittleEndian.Uint64(p))
	p = p[8:]
	q.meta.WriterOffset = int(binary.LittleEndian.Uint64(p))
	p = p[8:]
	q.meta.ReaderOffset = int(binary.LittleEndian.Uint64(p))
	return nil
}

func (q *FQueue) dumpMeta(meta *meta) error {
	var p = q.metaPtr
	copy(p, []byte(magic))
	p = p[len(magic):]
	p[0] = byte(meta.DumpFlag)
	p = p[1:]
	binary.LittleEndian.PutUint64(p, uint64(meta.FSize))
	p = p[8:]
	binary.LittleEndian.PutUint64(p, uint64(meta.Limit))
	p = p[8:]
	binary.LittleEndian.PutUint64(p, uint64(meta.WriterBottom))
	p = p[8:]
	binary.LittleEndian.PutUint64(p, uint64(meta.WriterOffset))
	p = p[8:]
	binary.LittleEndian.PutUint64(p, uint64(meta.ReaderOffset))
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
	if q.DumpFlag == 0 {
		q.dumpMeta(&q.meta)
	}
	return
}
