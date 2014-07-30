package fqueue

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

var (
	MaxJournalFiles  = 5
	JournalFileLimit = 100 * M
)

const (
	lowmask  = 0x00000000ffffffff
	highmask = 0xffffffff00000000
)

type JournalQueue struct {
	emptyJournals  []*FQueue
	journalQueues  []*FQueue
	pushQueue      *FQueue
	popQueue       *FQueue
	seq            uint32 //low 32bits in id.
	newQueueEnable bool
	mutex          *sync.Mutex
	cond           *sync.Cond
	running        bool
	journalLimit   int
	dirPath        string
	dirPathSum     uint64 //use the high 32bits in id.
}

type sortJournals []*FQueue

func (sj sortJournals) Len() int           { return len(sj) }
func (sj sortJournals) Less(i, j int) bool { return sj[i].Id&lowmask > sj[j].Id&lowmask }
func (sj sortJournals) Swap(i, j int)      { sj[j], sj[i] = sj[i], sj[j] }

func (q *JournalQueue) emptyJournalsJob() {
	var fq *FQueue
	var err error

	if fq, err = newFQueue(filepath.Join(q.dirPath, fmt.Sprintf("%d.jat", q.seq)), (q.dirPathSum<<32)&highmask, getAlign(q.journalLimit)); err != nil {
		//should not be happend
		panic(err)
	}

	//push empty queue
	q.mutex.Lock()
	q.emptyJournals = append(q.emptyJournals, fq)
	q.newQueueEnable = true
	q.mutex.Unlock()
	//notify
	q.cond.Broadcast()
}

func (q *JournalQueue) tryPrepareEmptyJournal() {
	if q.newQueueEnable && len(q.emptyJournals) == 0 {
		if q.pushQueue.Contents > (q.pushQueue.Limit-MetaSize)/2 {
			q.newQueueEnable = false
			//create a new empty file
			go q.emptyJournalsJob()
		}
	}
}

func (q *JournalQueue) Push(p []byte) (err error) {
	var pq *FQueue
TRY:
	q.mutex.Lock()
	if !q.running {
		return ClosedQueue
	}
	pq = q.pushQueue
	q.tryPrepareEmptyJournal()
	q.mutex.Unlock()
	if err = pq.Push(p); err != nil {
		if err != NoSpace {
			return
		}
		q.mutex.Lock()
		//when acquired the lock. other thread alread changed the pushQueue. so check the pushQueue with old.
		if pq != q.pushQueue {
			q.mutex.Unlock()
			goto TRY
		}

		if len(q.journalQueues) == MaxJournalFiles-1 {
			q.mutex.Unlock()
			return
		}

		//put it to journal queue.
		if q.pushQueue != q.popQueue {
			q.journalQueues = append(q.journalQueues, q.pushQueue)
		}

		for len(q.emptyJournals) == 0 {
			q.cond.Wait()

		}
		if q.pushQueue != pq {
			q.mutex.Unlock()
			goto TRY
		}
		//pop the empty queue
		q.pushQueue = q.emptyJournals[0]
		q.emptyJournals = q.emptyJournals[1:]

		q.pushQueue.setID(q.getQueueId())
		q.seq++
		q.mutex.Unlock()
		goto TRY
	}

	return nil
}

func newJournalQueue(path string) (jq *JournalQueue, err error) {
	var st os.FileInfo
	if st, err = os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(path, 0775); err != nil {
				return
			}
		} else {
			return
		}
	} else {
		if !st.IsDir() {
			err = MustBeDirectory
			return
		}
	}

	dirPathCrc32 := uint64(crc32.ChecksumIEEE([]byte(path)))

	var journalLimit int
	var emptyJournals []*FQueue
	var journals []*FQueue
	var fq *FQueue
	filepath.Walk(path, func(p string, info os.FileInfo, e error) error {

		if filepath.Ext(p) != ".jat" {
			return nil
		}
		if info.IsDir() {
			return filepath.SkipDir
		}
		if info.Size() < MetaSize || info.Size()%MetaSize != 0 {
			return nil
		}

		var err error
		var fd *os.File = nil
		var readOnlyMeta *meta
		if fd, err = os.OpenFile(p, os.O_RDONLY, 0664); err != nil {
			return err
		}
		defer fd.Close()
		if readOnlyMeta, err = getReadOnlyMeta(fd); err != nil {
			return nil
		}
		if readOnlyMeta.Id&highmask != dirPathCrc32<<32 {
			return nil
		}
		if journalLimit != 0 && journalLimit != readOnlyMeta.Limit {
			panic("limit is different, can't support now.")
		} else {
			journalLimit = readOnlyMeta.Limit
		}
		fq = &FQueue{
			qMutex: &sync.Mutex{},
		}
		if err = fq.loadMeta(p, readOnlyMeta.Id); err != nil {
			//don't happend.
			panic(err)
		}
		fq.running = true
		println(p, readOnlyMeta.Id&lowmask)
		if readOnlyMeta.Id&lowmask == 0 {
			emptyJournals = append(emptyJournals, fq)
		} else {
			journals = append(journals, fq)
		}
		return nil
	})
	q := &JournalQueue{
		seq:            2,
		newQueueEnable: true,
		mutex:          &sync.Mutex{},
	}
	q.cond = sync.NewCond(q.mutex)
	q.dirPath = path
	q.dirPathSum = dirPathCrc32
	if len(journals) > 0 {
		q.journalLimit = journalLimit - MetaSize
		//sort by raise order
		sort.Sort(sortJournals(journals))
		//use the bigest id for push queue and use the lowest id for pop queue.
		q.pushQueue = journals[0]
		if len(journals) == 0 {
			q.popQueue = q.pushQueue
		} else {
			q.popQueue = journals[len(journals)-1]
			journals = journals[:len(journals)-1]
		}
		q.seq = uint32(q.pushQueue.Id&lowmask) + 1
	} else {
		q.journalLimit = JournalFileLimit
		if fq, err = newFQueue(filepath.Join(path, fmt.Sprintf("%d.jat", 0x1)), (dirPathCrc32<<32)|0x1, getAlign(q.journalLimit)); err != nil {
			return
		}
		q.pushQueue = fq
		q.popQueue = fq
	}
	q.emptyJournals = emptyJournals
	q.journalQueues = journals
	println(len(emptyJournals))
	q.running = true
	q.tryPrepareEmptyJournal()
	jq = q
	return
}

func NewJournalQueue(path string) (jq *JournalQueue, err error) {
	var absPath string
	if absPath, err = filepath.Abs(path); err != nil {
		return
	}
	absPath = filepath.ToSlash(absPath)
	return newJournalQueue(absPath)
}

func (q *JournalQueue) getQueueId() uint64 {
	return ((q.dirPathSum << 32) & highmask) | (uint64(q.seq) & lowmask)
}

func (q *JournalQueue) Close() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.running = false
	for !q.newQueueEnable {
		q.cond.Wait()
	}
	if q.pushQueue != nil {
		q.pushQueue.Close()
	}
	if q.popQueue != nil && q.popQueue != q.pushQueue {
		q.popQueue.Close()
	}
	for _, q := range q.journalQueues {
		q.Close()
	}
	for _, q := range q.emptyJournals {
		q.Close()
	}
}

func (q *JournalQueue) Pop() (sli []byte, err error) {
	var pq *FQueue
TRY:
	q.mutex.Lock()
	if !q.running {
		err = ClosedQueue
		return
	}
	pq = q.popQueue
	q.mutex.Unlock()
	if sli, err = pq.Pop(); err != nil {
		if err != QueueEmpty {
			return
		}
		q.mutex.Lock()
		//when acquired the lock. other thread alread changed the popQueue. so check the popQueue with old.
		if pq != q.popQueue {
			q.mutex.Unlock()
			goto TRY
		}
		//if alread is same as push queue, there is no data, just return.
		if pq == q.pushQueue {
			q.mutex.Unlock()
			return
		}

		//release the journal File
		q.popQueue.reset((q.dirPathSum << 32) & highmask)
		q.emptyJournals = append(q.emptyJournals, q.popQueue)
		if len(q.journalQueues) == 0 {
			q.popQueue = q.pushQueue
		} else {
			q.popQueue = q.journalQueues[0]
			q.journalQueues = q.journalQueues[1:]
		}
		q.mutex.Unlock()
		goto TRY
	}
	return
}
