package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	//"net"
	"bytes"
	"encoding/gob"
	"time"
	"math/rand"
	"log"
)

// import "bytes"
// import "encoding/gob"
type Role int
const (
	FOLLOWER Role = 1 + iota
	CANDICATE
	LEADER
)

// const for timer
const (
	HEARTBEATINTERVAL int = 20
	HEARTHEATTIMEOUTBASE int = 150
	HEARTBEATTIMEOUTRANGE int = 150
	ELECTIONTIMEOUTBASE int = HEARTHEATTIMEOUTBASE
	ELECTIONTIMEOUTRANGE int = HEARTBEATTIMEOUTRANGE
)

type Entry struct {
	Term 	uint64
	Command interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persist
	currentTerm 	uint64
	votedFor	int
	log		[]Entry

	// mutable
	commitIdx	uint64
	lastApplied	uint64

	// leader only
	nextIdx 	[]uint64
	matchIdx 	[]uint64

	// memory
	applyCh chan ApplyMsg
	heartBeatCh chan *AppendEntriesArgs
	rand 	*rand.Rand
	role 	Role

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	rf.mu.Unlock()
	return int(rf.currentTerm), rf.role == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.log)
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term		uint64
	CandidateId 	int
	LastLogIdx 	uint64
	LastLogTerm	uint64
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term	uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term 		uint64
	LeaderId 	int
	PrevLogIdx 	uint64
	PrevLogTerm	uint64
	Entries		[]Entry
	LeaderCommit	uint64
}


type AppendEntriesReply struct {
	Term 	uint64
	Success bool
}



func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIdxV := uint64(len(rf.log) - 1)
	lastLogTermV := rf.log[lastLogIdxV].Term


	deny := false

	if rf.currentTerm > args.Term {
		// candidate's Term is stale
		deny = true
	}else if lastLogTermV > args.LastLogTerm ||
			(lastLogTermV == args.LastLogTerm &&
				lastLogIdxV > args.LastLogIdx){
		// voting server's log is more complete ||
		// (lastTermV > lastTermC) ||
		// (lastTermV == lastTermC) && (lastIndexV > lastIndexC)
		deny = true
	}else if rf.currentTerm == args.Term && rf.votedFor >= 0 {
		// in this Term, voting server has already vote for someone
		deny = true
	}

	if(deny) {
		// send false ack
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// otherwise, grant vote
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	// rf.currentTerm = args.Term
	return
}

// TODO: now appendEntries is only used for heartbeat
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term == rf.currentTerm && args.LeaderId != rf.votedFor && rf.role == FOLLOWER {
		log.Fatalf("2 leaders in the same Term, Term: %v, leaders: %v %v\n", args.Term, args.LeaderId, rf.votedFor)
	}

	if rf.currentTerm > args.Term {
		// msg's term is stale
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// firstly, let's check the consistency
	/*
	logIdxCheck := args.PrevLogIdx
	logTermCheck := args.PrevLogTerm
	if logIdxCheck >= uint64(len(rf.log)) || logTermCheck != rf.log[logIdxCheck].Term {
		// consistency check fails
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	*/

	rf.heartBeatCh <- &args


}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// log.Println("send", args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	Term := -1
	isLeader := true


	return index, Term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// used by leader to send out heartbeat
func (rf *Raft) BroadcastHeartBeat() {
	waitTime := time.Duration(HEARTBEATINTERVAL)
	for {
		if rf.role != LEADER {
			log.Fatalf("call broadcast heartbeat, but I'm not a leader\n")
		}

		// send out heartheat every HEARTBEATINTERVAL ms
		timeout := make(chan bool, 1)
		go func() {
			time.Sleep(waitTime * time.Millisecond)
			timeout <- true
		}()


		staleSignal := make(chan bool, len(rf.peers) - 1)

		// broadcast heartheat in parallel
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				// escape myself
				continue
			}
			go func(server int) {
				// TODO: do we need to send check information in heartbeat?
				matchedLogIdx := rf.matchIdx[server]
				matchedTermIdx := rf.log[matchedLogIdx].Term
				//args := makeAppendEntriesArgs(rf.currentTerm, rf.me, matchedLogIdx, matchedTermIdx, Entry{}, rf.commitIdx)
				args := AppendEntriesArgs{rf.currentTerm, rf.me, matchedLogIdx, matchedTermIdx, make([]Entry, 0), rf.commitIdx}
				reply := new(AppendEntriesReply)
				ok := rf.sendAppendEntries(server, args, reply)

				// reply shows that my Term is stale
				// prepare for the role change
				if(ok && reply.Term > rf.currentTerm) {
					staleSignal <- true
				}
			}(i)

		}

		endLoop := false
		for !endLoop{
			select {
			case <-staleSignal:
			// my Term is stale
			// convert to follower stage
				rf.mu.Lock()
				rf.role = FOLLOWER
				rf.nextIdx = nil
				rf.matchIdx = nil
				rf.mu.Unlock()
				log.Printf("leader %v is stale, turns to follower\n", rf.me)
				go rf.HeartBeatTimer()
				return
			case msg := <-rf.heartBeatCh:
			// get a heart beat from others
				if rf.currentTerm == msg.Term {
					// in this Term, there are 2 leaders
					// impossible
					log.Fatalf("in leader %v's broadcast, receive the same heartbeat Term, value: %v leader: %v\n", rf.me, msg.Term, msg.LeaderId)
				}else if rf.currentTerm < msg.Term {
					// heart beat from a superior leader
					rf.mu.Lock()
					rf.role = FOLLOWER
					rf.currentTerm = msg.Term
					rf.votedFor = msg.LeaderId
					rf.nextIdx = nil
					rf.matchIdx = nil
					rf.mu.Unlock()
					log.Printf("leader %v finds a superior leader %v, turns to follower\n", rf.me, rf.votedFor)
					go rf.HeartBeatTimer()
					return
				}

			case <-timeout:
				// begin another broadcast round
				endLoop = true
				break
			}
		}
	}
}

// issued a new election Term to become leader, by a candidate
func (rf *Raft) Election() {
	// turn into candidate
	// increase current Term
	// vote for myself
	rf.mu.Lock()
	rf.role = CANDICATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()

	log.Printf("new election begin in %v, Term %v\n", rf.me, rf.currentTerm)
	lastLogIdx := uint64(len(rf.log) - 1)
	lastLogTerm := rf.log[lastLogIdx].Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIdx, lastLogTerm}


	recBuff := make(chan *RequestVoteReply, 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// escape myself
			continue
		}

		// send requestVote in parallel
		go func(server int) {
			reply := new(RequestVoteReply)
			reply.Term = 0
			reply.VoteGranted = false
			rf.sendRequestVote(server, args, reply)
			recBuff <- reply
		}(i)
	}

	// signal: wins the election
	winSignal := make(chan bool, 1)
	// signal: my current Term is out of date
	staleSignal := make(chan *RequestVoteReply, 1)

	go func(){
		// get an approve from myself
		approveNum := 1
		for i := 0; i < len(rf.peers) - 1; i++{
			reply := <- recBuff
			if reply.VoteGranted{
				approveNum++
				if approveNum > len(rf.peers) / 2{
					winSignal <- true
					break
				}
			}else if reply.Term > rf.currentTerm{
				staleSignal <- reply
				break
			}
		}
	}()

	// election timer
	waitTime := time.Duration(ELECTIONTIMEOUTBASE+ rf.rand.Intn(ELECTIONTIMEOUTRANGE))
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(waitTime * time.Millisecond)
		timeout <- true
	}()

	// loop until win, fail, or timeout
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for {
		select {
		case msg := <- rf.heartBeatCh:
			if msg.Term < rf.currentTerm {
				// receive stale heartbeat
				// ignore
			}

			// fail the election
			// get heartbeat from other leader
			rf.currentTerm = msg.Term
			rf.role = FOLLOWER
			rf.votedFor = msg.LeaderId
			go rf.HeartBeatTimer()
			log.Printf("candidate %v becomes follower\n", rf.me)
			return
		case <-winSignal:
			rf.role = LEADER

			// reinit nextIdx, matchIdx
			rf.nextIdx = make([]uint64, len(rf.peers))
			rf.matchIdx = make([]uint64, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIdx[i] = uint64(len(rf.log))
				rf.matchIdx[i] = 0
			}
			log.Printf("candidate %v becomes leader in Term %v\n", rf.me, rf.currentTerm)
			go rf.BroadcastHeartBeat()
			return
		case reply := <-staleSignal:
			// discover a new Term
			// turn into follower state
			// another kind of failure
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.votedFor = -1
			go rf.HeartBeatTimer()
			return
		case <-timeout:
			// fire another election Term
			log.Printf("election timeout in candidate %v term %v\n", rf.me, rf.currentTerm)
			go rf.Election()
			return
		}
	}

}

// used by follower
func (rf *Raft) HeartBeatTimer() {
	// in the same Term, we use the same timeout
	waitTime := time.Duration(HEARTHEATTIMEOUTBASE + rf.rand.Intn(HEARTBEATTIMEOUTRANGE))

	for {

		if rf.role != FOLLOWER {
			log.Fatal("call heartBeatTimer, but I'm not a follower")
		}

		timeout := make(chan bool, 1)

		go func() {
			time.Sleep(waitTime * time.Millisecond)
			timeout <- true
		}()

		// loop until time out or receive a correct heartbeat
		endLoop := false
		for !endLoop {
			select {
			case msg := <-rf.heartBeatCh:
				if rf.currentTerm > msg.Term {
					// stale heart beat
					// ignore and continue the loop
					log.Println("%v receive a stale heartbeat")
				}else if rf.votedFor != -1 && rf.currentTerm == msg.Term &&
					// illegal state
					rf.votedFor != msg.LeaderId {
					log.Fatalf("there are 2 leaders in the same Term. Term: %v, leader 1 %v leader 2 %v\n",
						rf.currentTerm, rf.votedFor, msg.LeaderId)
				}else {
					// receive a legal heartbeat
					// break the loop to wait next heartBeat
					rf.mu.Lock()
					rf.currentTerm = msg.Term
					rf.votedFor = msg.LeaderId
					rf.mu.Unlock()

					endLoop = true
				}
			case <-timeout:
				// time out, end the heartbeat timer
				// and fire a new election Term
				go rf.Election()
				return
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.Printf("new server %v is up\n", me)
	rf := &Raft{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)

	// insert a fake entry in the first log
	rf.log = append(rf.log, Entry{0, nil})

	rf.applyCh = applyCh
	rf.heartBeatCh = make(chan *AppendEntriesArgs, 1)
	rf.rand = rand.New(rand.NewSource(int64(rf.me)))
	rf.role = FOLLOWER

	// init server only elements
	rf.nextIdx = nil
	rf.matchIdx = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// begin from follower, expect to receive heartbeat
	go rf.HeartBeatTimer()
	return rf
}
