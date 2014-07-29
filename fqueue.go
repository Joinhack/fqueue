package fqueue

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"
)

const (
	K = 1024
	M = 1024 * K
	G = 1024 * M
)

const (
	MetaMask   = 0x1 //if set, the meta is already.
	MetaUnMask = ^MetaMask & 0xff
)

var (
	magic                  = "JFQ"
	metaStructSize uintptr = unsafe.Sizeof(meta{})
	magicLen       int     = len(magic)

	QueueLimit                         = 50 * M
	PrepareCall func(string, int, int) = simplePrepareCall()
)

func simplePrepareCall() func(string, int, int) {
	var n time.Time
	return func(path string, limit, now int) {
		if now == 0 {
			fmt.Println("preparing [", path, "]")
			n = time.Now()
			fmt.Print(".")
		} else if now%(5*M) == 0 {
			fmt.Print(".")
		}
		if now == limit {
			fmt.Println("100%")
			fmt.Println("finished, used time:", (time.Now().UnixNano()-n.UnixNano())/1000000, "ms")
		}
	}
}

type meta struct {
	Mask         byte
	Id           uint64
	WriterOffset int
	ReaderOffset int
	WriterBottom int
	Limit        int
	Contents     int
}

type Queue interface {
	Push([]byte) error
	Pop() ([]byte, error)
	Close()
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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	empty := make([]byte, 4096)

	if PrepareCall != nil {
		PrepareCall(path, limit, 0)
	}
	for i := 0; i < limit; {
		file.Write(empty)
		i += len(empty)
		//file queue prepared callback.
		if PrepareCall != nil {
			PrepareCall(path, limit, i)
		}
	}
}

func (q *FQueue) setID(id uint64) {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	q.Mask &= MetaUnMask
	q.Id = id
	q.Mask |= MetaMask
	*q.meta1 = *q.meta
}

func (q *FQueue) reset(id uint64) {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	q.Mask &= MetaUnMask
	q.meta.ReaderOffset = MetaSize
	q.meta.WriterOffset = MetaSize
	q.meta.Id = id
	q.meta.WriterBottom = q.meta.WriterOffset
	q.Mask |= MetaMask
	*q.meta1 = *q.meta
}

func (q *FQueue) Push(p []byte) error {
	var plen = len(p)
	if plen == 0 {
		return nil
	}
	var needSpace = plen + 2
	var err error
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	if !q.running {
		return ClosedQueue
	}
	if needSpace+q.Contents > q.Limit-MetaSize {
		return NoSpace
	}
	if (q.WriterOffset < q.WriterBottom) &&
		(q.WriterOffset+needSpace >= q.ReaderOffset) {
		return NoSpace
	}
	q.Mask &= MetaUnMask
	if q.WriterOffset+needSpace >= q.Limit {
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

func newFQueue(path string, id uint64, fileLimit int) (fq *FQueue, err error) {

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
			q.Id = id
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
		if err = q.loadMeta(path, id); err != nil {
			return
		}
	}
	fq = q
	return
}

func NewFQueue(path string) (fq *FQueue, err error) {
	var absPath string
	if absPath, err = filepath.Abs(path); err != nil {
		return
	}
	absPath = filepath.ToSlash(absPath)
	return newFQueue(absPath, uint64(crc32.ChecksumIEEE([]byte(absPath))), getAlign(QueueLimit))
}

func (q *FQueue) Close() {
	q.qMutex.Lock()
	defer q.qMutex.Unlock()
	q.running = false
	if q.meta1 != nil && q.meta != nil {
		//merge meta
		*q.meta1 = *q.meta
	}
	if len(q.ptr) > 0 {
		unmap(q.ptr)
	}
	if q.fd != nil {
		q.fd.Close()
	}
}

func getReadOnlyMeta(fd *os.File) (meta *meta, err error) {
	var n int
	metaSli := make([]byte, MetaSize)
	if n, err = fd.Read(metaSli); err != nil {
		return
	}
	if n != MetaSize {
		err = InvaildReadn
		return
	}
	//find the mmap size info in the meta.
	readonlyMeta1 := metaMapper(uintptr(magicLen), metaSli)
	readonlyMeta2 := metaMapper(uintptr(magicLen)+metaStructSize, metaSli)

	if string(metaSli[:magicLen]) != magic {
		err = InvaildMeta
		return
	}
	if readonlyMeta1.Mask&MetaMask == 0 {
		meta = readonlyMeta2
	} else {
		meta = readonlyMeta1
	}
	return
}

func (q *FQueue) loadMeta(path string, id uint64) (err error) {
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
	var readonlyMeta *meta
	if readonlyMeta, err = getReadOnlyMeta(q.fd); err != nil {
		return
	}

	if readonlyMeta.Id != id {
		err = InvaildMeta
		return
	}
	//reset file.
	if _, err = q.fd.Seek(0, os.SEEK_SET); err != nil {
		return
	}
	q.ptr, err = mmap(q.fd.Fd(), 0, readonlyMeta.Limit, RDWR)
	if err != nil {
		return
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
	if !q.running {
		err = ClosedQueue
		return
	}
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
