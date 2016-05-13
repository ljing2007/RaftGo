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
	"io/ioutil"
)

const dbg bool = true

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
	HEARTBEATINTERVAL int = 60
	HEARTHEATTIMEOUTBASE int = 150
	HEARTBEATTIMEOUTRANGE int = 150
	ELECTIONTIMEOUTBASE int = HEARTHEATTIMEOUTBASE
	ELECTIONTIMEOUTRANGE int = HEARTBEATTIMEOUTRANGE
)

type Entry struct {
	Term 	uint64
	Command interface{}
}

type TermLeader struct {
	Term uint64
	LeaderId int
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
	votedFor	TermLeader
	log		[]Entry

	// mutable
	commitIdx	uint64
	lastApplied	uint64

	// leader only
	nextIdx 	[]uint64
	matchIdx 	[]uint64
	pLocks		[]sync.Mutex

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

	if rf.currentTerm > args.Term || rf.votedFor.Term > args.Term {
		// candidate's Term is stale
		log.Printf("%v denies the vote from %v because stale\n", rf.me, args.CandidateId)
		deny = true
	}else if lastLogTermV > args.LastLogTerm ||
			(lastLogTermV == args.LastLogTerm &&
				lastLogIdxV > args.LastLogIdx){
		// voting server's log is more complete ||
		// (lastTermV > lastTermC) ||
		// (lastTermV == lastTermC) && (lastIndexV > lastIndexC)
		log.Printf("%v denies the vote from %v because more complete\n", rf.me, args.CandidateId)
		deny = true
	}else if rf.votedFor.Term == args.Term && rf.votedFor.LeaderId >= 0 {
		// in this Term, voting server has already vote for someone
		log.Printf("%v denies the vote from %v because already vote\n", rf.me, args.CandidateId)
		deny = true
	}

	if(deny) {
		// send false ack
		reply.Term = rf.votedFor.Term
		reply.VoteGranted = false
		return
	}

	// otherwise, grant vote
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.votedFor = TermLeader{args.Term, args.CandidateId}
	rf.persist()
	return
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("%v term %v receive %v\n", rf.me, rf.currentTerm, args.Term)
	if args.Term == rf.votedFor.Term && args.LeaderId != rf.votedFor.LeaderId &&
		rf.role == FOLLOWER && rf.votedFor.LeaderId != -1 {
		log.Fatalf("2 leaders in the same Term, Term: %v, leaders: %v %v\n", args.Term, args.LeaderId, rf.votedFor)
	}

	if rf.currentTerm > args.Term {
		// msg's term is stale
		reply.Success = false
		reply.Term = rf.currentTerm
		log.Printf("%v got a stale term from %v\n", rf.me, args.LeaderId)
		return
	}

	// treat all messages, whose term >= rf.currentTerm, as a heartBeat
	go func() {
		rf.heartBeatCh <- &args
	}()

	// then, let's check the consistency
	logIdxCheck := args.PrevLogIdx
	logTermCheck := args.PrevLogTerm
	if logIdxCheck >= uint64(len(rf.log)) || logTermCheck != rf.log[logIdxCheck].Term {
		// consistency check fails
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.commitIdx > args.LeaderCommit {
		if rf.votedFor.LeaderId != args.LeaderId {
			log.Fatalln("try to delete committed entry")
		}
		return
	}

	// pass consistency check
	// delete and append entries safely
	rf.log = rf.log[ : logIdxCheck + 1]
	rf.log = append(rf.log, args.Entries...)

	// commit locally
	for cId := rf.commitIdx + 1; cId <= args.LeaderCommit; cId++ {
		if cId >= uint64(len(rf.log))  {
			break
		}
		rf.commitIdx = cId
		log.Printf("%v commit %v %v", rf.me, cId, rf.log[cId])
		rf.applyCh <- ApplyMsg{int(cId), rf.log[cId].Command, false, nil}
	}

	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true
	return
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

func(rf *Raft) LeaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return
	}
	// find the first entry in current term
	minIdx := 0
	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term == rf.currentTerm {
			minIdx = i
		}else if rf.log[i].Term < rf.currentTerm {
			break
		}
	}

	if minIdx == 0 {
		// can't find entry in current term
		// unsafe to commit
		return
	}

	// find the safe upper bound
	upperBound := rf.commitIdx
	for minIdx < len(rf.log)  {
		replicatedNum := 1
		safe := false

		// loop all peers to check whether this entry is replicated
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if int(rf.matchIdx[i]) >= minIdx {
				// entry minIdx has replicated in server i
				replicatedNum++
				if replicatedNum > len(rf.peers) / 2 {
					// replicated in the majority
					safe = true
					upperBound = uint64(minIdx)
					minIdx++
					break
				}
			}
		}
		if !safe {
			break
		}
	}

	cId := rf.commitIdx + 1
	//log.Printf("leader %v upperbound %v min %v\n", rf.me, upperBound, minIdx)
	for cId <= upperBound {
		if cId >= uint64(len(rf.log)) {
			log.Fatalln("out of bound")
		}
		log.Printf("leader %v commit %v %v", rf.me, cId, rf.log[cId])
		rf.applyCh <- ApplyMsg{int(cId), rf.log[cId].Command, false, nil}
		rf.commitIdx = cId
		rf.persist()
		cId++
	}

}


func(rf *Raft) Sync(server int) (bool, uint64) {
	rf.pLocks[server].Lock()
	var matchedLogIdx uint64
	var entries []Entry
	if rf.nextIdx[server] == rf.matchIdx[server] + 1 {
		// consistent
		matchedLogIdx = rf.matchIdx[server]

		if matchedLogIdx + 1 < uint64(len(rf.log)){
			entries = rf.log[matchedLogIdx + 1 : ]
		}

	}else {
		// haven't achieve consistency
		// use nextIdx and empty entries
		matchedLogIdx = rf.nextIdx[server] - 1
	}

	rf.pLocks[server].Unlock()


	matchedTermIdx := rf.log[matchedLogIdx].Term
	//args := makeAppendEntriesArgs(rf.currentTerm, rf.me, matchedLogIdx, matchedTermIdx, Entry{}, rf.commitIdx)
	args := AppendEntriesArgs{rf.currentTerm, rf.me, matchedLogIdx, matchedTermIdx, entries, rf.commitIdx}
	reply := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(server, args, reply)


	if !ok {

		return false, 0
	}

	rf.pLocks[server].Lock()
	defer rf.pLocks[server].Unlock()
	if reply.Success {
		matchedLogIdx = matchedLogIdx + uint64(len(entries))
		if rf.matchIdx[server] != matchedLogIdx{
			log.Printf("%v matched become %v\n", server, matchedLogIdx)
		}
		rf.matchIdx[server] = matchedLogIdx
		rf.nextIdx[server] = matchedLogIdx + 1

	} else if reply.Term == rf.currentTerm {
		if matchedLogIdx == 0 {
			//log.Fatalln("matchedLogIdx: 0, fail")
		}else {
			rf.nextIdx[server] = matchedLogIdx
		}
	}
	rf.LeaderCommit()

	return true, reply.Term
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return -1, -1, false
	}

	for idx, entry := range rf.log {
		if entry.Command == command {
			return idx, int(entry.Term), true
		}
	}

	index := len(rf.log)
	Term := rf.currentTerm

	rf.log = append(rf.log, Entry{Term, command})

	rf.matchIdx[rf.me] = uint64(len(rf.log)) - 1
	rf.nextIdx[rf.me] = uint64(len(rf.log))

	rf.persist()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go rf.Sync(i)
	}

	log.Printf("%v start in leader %v, index %v, term %v\n", command, rf.me, index, Term)
	return index, int(Term), true
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
				ok, term := rf.Sync(server)
				if ok && term > rf.currentTerm {
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
				// rf.nextIdx = nil
				// rf.matchIdx = nil
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
					rf.votedFor = TermLeader{msg.Term, msg.LeaderId}
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
func (rf *Raft) Election(electionTerm uint64) {
	// turn into candidate
	// increase current Term
	// vote for myself
	rf.mu.Lock()
	if rf.currentTerm >= electionTerm {
		// race
		log.Fatalf("%v's term is updated by someone, but not be caught\n", rf.me)
	}
	rf.role = CANDICATE
	rf.currentTerm = electionTerm
	rf.votedFor = TermLeader{electionTerm, rf.me}
	rf.mu.Unlock()

	log.Printf("new election begin in %v, Term %v\n", rf.me, electionTerm)
	lastLogIdx := uint64(len(rf.log) - 1)
	lastLogTerm := rf.log[lastLogIdx].Term
	args := RequestVoteArgs{electionTerm, rf.me, lastLogIdx, lastLogTerm}


	type Rec struct {
		ok bool
		reply *RequestVoteReply
	}
	recBuff := make(chan Rec, 1)
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
			ok := rf.sendRequestVote(server, args, reply)
			recBuff <- Rec {ok, reply}
		}(i)
	}

	// signal: wins the election
	winSignal := make(chan bool, 1)
	// signal: my current Term is out of date
	staleSignal := make(chan *RequestVoteReply, 1)
	failSingal := make(chan bool)
	go func(){
		// get an approve from myself
		approveNum := 1
		denyNum := 0
		for i := 0; i < len(rf.peers) - 1; i++{
			rec := <- recBuff
			if !rec.ok {
				continue
			}
			if rec.reply.VoteGranted{
				approveNum++
				if approveNum > len(rf.peers) / 2{
					winSignal <- true
					break
				}
			}else{
				if rec.reply.Term > rf.currentTerm {
					staleSignal <- rec.reply
					break
				}

				denyNum++
				if denyNum > len(rf.peers) / 2 {
					failSingal <- true
					break
				}

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
	for {
		select {
		case msg := <- rf.heartBeatCh:
			if msg.Term < rf.currentTerm {
				// receive stale heartbeat
				// ignore
				break
			}


			// fail the election
			// get heartbeat from other leader
			rf.mu.Lock()
			rf.currentTerm = msg.Term
			rf.role = FOLLOWER
			rf.votedFor = TermLeader{msg.Term, msg.LeaderId}
			rf.mu.Unlock()
			go rf.HeartBeatTimer()
			log.Printf("candidate %v becomes follower\n", rf.me)
			return
		case <-winSignal:
			rf.mu.Lock()
			rf.role = LEADER

			// reinit nextIdx, matchIdx
			rf.nextIdx = make([]uint64, len(rf.peers))
			rf.matchIdx = make([]uint64, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIdx[i] = uint64(len(rf.log))
				rf.matchIdx[i] = 0
			}
			rf.mu.Unlock()
			log.Printf("candidate %v becomes leader in Term %v\n", rf.me, rf.currentTerm)
			go rf.BroadcastHeartBeat()

			return
		case <- failSingal:
			rf.mu.Lock()
			rf.role = FOLLOWER
			rf.votedFor = TermLeader{rf.currentTerm, -1}
			rf.mu.Unlock()
			rf.persist()
			go rf.HeartBeatTimer()
			return

		case reply := <-staleSignal:
			rf.mu.Lock()

			// discover a new Term
			// turn into follower state
			// another kind of failure
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.votedFor = TermLeader{reply.Term, -1}
			rf.mu.Unlock()
			rf.persist()
			go rf.HeartBeatTimer()
			return
		case <-timeout:
			// fire another election Term
			log.Printf("election timeout in candidate %v term %v\n", rf.me, rf.currentTerm)
			go rf.Election(electionTerm + 1)
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
			log.Fatalln("call heartBeatTimer, but I'm not a follower")
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
				}else if rf.votedFor.LeaderId != -1 && rf.votedFor.Term == msg.Term &&
						rf.votedFor.LeaderId != msg.LeaderId {
					// illegal state
					log.Fatalf("there are 2 leaders in the same Term. Term: %v, leader 1 %v leader 2 %v\n",
						rf.currentTerm, rf.votedFor, msg.LeaderId)
				}else {
					// receive a legal heartbeat
					// break the loop to wait next heartBeat
					rf.mu.Lock()
					rf.currentTerm = msg.Term
					rf.votedFor = TermLeader{msg.Term, msg.LeaderId}
					rf.persist()
					rf.mu.Unlock()
					endLoop = true
				}
			case <-timeout:
				// time out, end the heartbeat timer
				// and fire a new election Term
				go rf.Election(rf.currentTerm + 1)
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
	if !dbg {
		log.SetOutput(ioutil.Discard)
	}
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("new server %v is up\n", me)
	rf := &Raft{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialization code here.
	rf.currentTerm = 0
	rf.votedFor = TermLeader{0, -1}
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
	rf.pLocks = make([]sync.Mutex, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// begin from follower, expect to receive heartbeat
	go rf.HeartBeatTimer()
	return rf
}
