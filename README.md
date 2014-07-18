FQueue
==================================
The fqueue for golang.
(Now don't support windows.)


Features
==================================
* Fast
* Persistence
* Rolling File

Install
==================================
1. Set the GOPATH 

2. Install the peony. 

		$ go get github.com/joinhack/fqueue

Setting
==================================
Before create a queue.

fqueue.FileLimit: set the file size

fqueue.DefaultDumpFlag: set the meta data dump strategy.

        0 dump meta immediately, this is the default value
        
        1 dump meta per second.


Test
==================================
$ go test -bench="." -benchtime=0s


Test Result
==================================

BenchmarkPush	 1000000	       743 ns/op	 158.78 MB/s
BenchmarkPop	 1000000	       591 ns/op	 199.61 MB/s
