package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"os"
	"io/ioutil"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type PendingOps struct {
	Req Op
	Success chan bool
}



type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data	map[string]string
	pendingOps	map[int]*PendingOps

	logger	raft.Logger
}

func (kv *RaftKV) receiveApply() {
	for {
		msg := <-kv.applyCh

		kv.logger.Trace.Printf("get apply: %v in server %v\n", msg, kv.me)

		idx, req := msg.Index, msg.Command.(Op)

		op, ok := kv.pendingOps[idx]

		if !ok {
			kv.logger.Trace.Println("server %v doesn't have this pending op")
			continue
		}

		if op.Req.RequestId != req.RequestId {
			op.Success <- false
		}else {
			switch op.Req.Type {
			case PUT:
				kv.data[op.Req.Key] = op.Req.Value
			case APPEND:
				kv.data[op.Req.Key] += op.Req.Value
			}
			op.Success <- true
		}
		delete(kv.pendingOps, idx)
	}
}


func (kv *RaftKV) ExecuteRequest(args Op, reply *Reply) {
	idx, _, ok := kv.rf.Start(args)
	if !ok {
		reply.Success = false
		return;
	}

	kv.logger.Trace.Printf("start %v in %v, is leader: %v, idx %v\n", args, kv.me, ok, idx)

	op := new(PendingOps)
	op.Req = args
	op.Success = make(chan bool, 1)

	if val, ok := kv.pendingOps[idx]; ok {
		val.Success <- false
	}
	kv.pendingOps[idx] = op

	timmer := time.NewTimer(time.Second * 3)
	select {
	case <-timmer.C:
		reply.Success = false
		kv.logger.Trace.Println("time out")
		return
	case ok = <- op.Success:
		reply.Success = ok
		if ok && args.Type == GET {
			reply.Value = kv.data[args.Key]
		}
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.


	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int]*PendingOps)

	if Debug > 0 {
		kv.logger.InitLogger(os.Stdout, os.Stdout, os.Stderr, os.Stderr)
	}else {
		kv.logger.InitLogger(ioutil.Discard, ioutil.Discard, os.Stderr, os.Stderr)
	}
	go kv.receiveApply()
	return kv
}
