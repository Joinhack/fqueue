Fqueue
==================================
The fqueue for golang 


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

BenchmarkPush	 1000000	       965 ns/op	  98.40 MB/s

BenchmarkPop	 1000000	       588 ns/op	 161.50 MB/s





