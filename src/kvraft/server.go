package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"os"
	"io/ioutil"
	"time"
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

	data		map[string]string	// kv store data
	pendingOps	map[int]*PendingOps	// pending requests
	stamps		map[int64]int64		// client id --> latest request id

	logger	raft.Logger
}

// get a committed msg from Raft
func (kv *RaftKV) receiveApply() {
	for {
		msg := <-kv.applyCh

		kv.logger.Trace.Printf("get apply: %v in server %v\n", msg, kv.me)

		idx, req := msg.Index, msg.Command.(Op)

		kv.mu.Lock()

		op, p_ok := kv.pendingOps[idx]
		stamp, s_ok := kv.stamps[req.ClientId]

		if (s_ok && stamp >= req.RequestId) {
			// already execute this cmd, ignore it
			// kv.logger.Warning.Printf("get stale cmd, server's stamp %v, req's stamp %v\n", stamp, req.RequestId)
		} else {
			// haven't execute this request
			kv.logger.Trace.Printf("req type %v\n", req.Type)
			switch req.Type {
			case PUT:
				kv.data[req.Key] = req.Value
			case APPEND:
				kv.data[req.Key] += req.Value
			}
			kv.stamps[req.ClientId] = req.RequestId
		}

		if !p_ok {
			kv.mu.Unlock()
			continue
		}

		// send back execute result
		if op.Req.RequestId != req.RequestId || op.Req.ClientId != req.ClientId {
			op.Success <- false
		}else {
			if op.Req.Type == GET {
				op.Req.Value = kv.data[op.Req.Key]
			}
			op.Success <- true
		}
		delete(kv.pendingOps, idx)
		kv.mu.Unlock()
	}
}

// response to the client request
func (kv *RaftKV) ExecuteRequest(args Op, reply *Reply) {

	// send the request to raft
	idx, _, ok := kv.rf.Start(args)
	if !ok {
		// I'm not leader, reject this reuest
		reply.Success = false
		return;
	}

	kv.logger.Trace.Printf("start %+v in %v, is leader: %v, idx %v\n", args, kv.me, ok, idx)

	// save this request to pending ops
	op := new(PendingOps)
	op.Req = args
	op.Success = make(chan bool, 1)

	kv.mu.Lock()
	if val, ok := kv.pendingOps[idx]; ok {
		val.Success <- false
		kv.logger.Warning.Println("alraedy have a log in this idx")
	}
	kv.pendingOps[idx] = op
	kv.mu.Unlock()

	// whether timing out or executed successfully
	timmer := time.NewTimer(time.Second)
	select {
	case <-timmer.C:
		reply.Success = false
		kv.logger.Warning.Printf("time out for the args %+v\n", args)
		return
	case ok = <- op.Success:
		reply.Success = ok
		if ok && args.Type == GET {
			reply.Value = op.Req.Value
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int]*PendingOps)
	kv.stamps = make(map[int64]int64)

	if Debug > 0 {
		kv.logger.InitLogger(os.Stdout, os.Stdout, os.Stderr, os.Stderr)
	}else {
		kv.logger.InitLogger(ioutil.Discard, ioutil.Discard, os.Stderr, os.Stderr)
	}
	go kv.receiveApply()
	return kv
}
