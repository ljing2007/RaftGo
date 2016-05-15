package raft

import (
	"log"
	"time"
)


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

	Trace.Printf("new election begin in %v, Term %v\n", rf.me, rf.currentTerm)
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
			Trace.Printf("in election %v get reply %v\n", rf.me, reply)
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
			Trace.Printf("candidate %v becomes follower\n", rf.me)
			return
		case <-winSignal:
			rf.mu.Lock()
			rf.role = LEADER

		// reinit nextIdx, matchIdx
			rf.nextIdx = make([]uint64, len(rf.peers))
			rf.matchIdx = make([]uint64, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIdx[i] = uint64(len(rf.log))
				rf.matchIdx[i] = 0
			}
			rf.mu.Unlock()
			Info.Printf("candidate %v becomes leader in Term %v\n", rf.me, rf.currentTerm)
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
			Trace.Printf("election timeout in candidate %v term %v\n", rf.me, rf.currentTerm)
			go rf.Election(electionTerm + 1)
			return
		}
	}

}

