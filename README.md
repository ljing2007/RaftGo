# RaftGo
A project for learning Golang.

Forked from [MIT's 6.824 (Distributed System)](https://pdos.csail.mit.edu/6.824/), original code: git://g.csail.mit.edu/6.824-golabs-2016

## Directory Tree
* src/raft: implementation of [**The Raft Consensus Algorithm**](http://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14)
	* under develop
	
* src/mapreduce: MapReduce framework, used for getting familiar with the source and Go.
	* implement sequential and distributed MapReduce framework
	* handle worker failure
	* implement 2 MapReduce task: word-count, and invert-list
