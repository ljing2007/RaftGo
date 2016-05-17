# RaftGo
A project for learning Golang.

Forked from [MIT's 6.824 (Distributed System)](https://pdos.csail.mit.edu/6.824/), original code: git://g.csail.mit.edu/6.824-golabs-2016

## Directory Tree
* src/kvraft: a Key-Value store based on Raft
	* under develop

* src/raft: implementation of [**The Raft Consensus Algorithm**](http://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14)
	* almost supports all features described in the paper:
		* leader election
		* append entries
		* log consistence
		* recovery after crash
		* resilient to unreliable network such as network partition, message reorder and message lost
	* note1: cluster membership must be static
	* note2: test_test.go is the test file of this project. Please only run part of these test functions at one time, otherwise, it may throw the error of running too many go routines.
		
	
* src/mapreduce: MapReduce framework, used for getting familiar with the source and Go.
	* implement sequential and distributed MapReduce framework
	* handle worker failure
	* implement 2 MapReduce task: word-count, and invert-list
