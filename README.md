fqueue
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

Test
==================================
$ go test -bench="." -benchtime=0s


Test Result
==================================

BenchmarkPush	 1000000	       965 ns/op	  98.40 MB/s

BenchmarkPop	 1000000	       588 ns/op	 161.50 MB/s





