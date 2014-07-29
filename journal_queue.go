package fqueue

import (
	"hash/crc32"
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
	emptyJournals []*FQueue
	journalQueues []*FQueue
	pushQueue     *FQueue
	popQueue      *FQueue
	seq           uint32 //low 32bits in id.
	producerChan  chan *FQueue
	customerChan  chan *FQueue
	mutex         *sync.Mutex
	wg            *sync.WaitGroup
	running       bool
	journalLimit  int
	dirPath       string
	dirPathSum    uint32 //use the high 32bits in id.
}

func (q *JournalQueue) emptyJournalsJob() {
	defer q.wg.Done()

	for q.running {
		var fq *FQueue
		var ok bool
		var err error
		if len(q.emptyJournals) == 0 {
			select {
			case fq, ok = <-q.producerChan:
				if !ok {
					return
				}
			}
		}
		if fq == nil && len(q.emptyJournals) == 0 {
			if fq, err = newFQueue(q.dirPath, uint64(q.dirPathSum<<32)&highmask, getAlign(q.journalLimit)); err != nil {
				panic(err)
			}
		}
		//append to end of empty queue.
		if fq != nil {
			q.emptyJournals = append(q.emptyJournals, fq)
		}
		//pop the fist element.
		fq = q.emptyJournals[0]
		q.emptyJournals = q.emptyJournals[1:]
		q.customerChan <- fq
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

	if len(q.emptyJournals) == 0 {
		if pq.Contents > (pq.Limit-MetaSize)/2 {
			//create a new empty file
			q.producerChan <- nil
		}
	}
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

		if q.pushQueue != q.popQueue {
			q.journalQueues = append(q.journalQueues, q.pushQueue)
		}
		q.pushQueue = <-q.customerChan

		q.pushQueue.setID(q.getQueueId())
		q.seq++
		q.mutex.Unlock()
		goto TRY
	}

	return nil
}

func NewJournalQueue() *JournalQueue {
	return nil
}

func (q *JournalQueue) getQueueId() uint64 {
	return ((uint64(q.dirPathSum) << 32) & highmask) | (uint64(q.seq) & lowmask)
}

func (q *JournalQueue) Close() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.running = false
	if q.pushQueue != nil {
		q.pushQueue.Close()
	}
	for _, q := range q.journalQueues {
		q.Close()
	}
	q.wg.Wait()
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
		q.popQueue.reset((uint64(q.dirPathSum) << 32) & highmask)
		q.producerChan <- q.popQueue
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
