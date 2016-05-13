package raft

import (
	"log"
	"time"
)


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
	//Trace.Printf("leader %v upperbound %v min %v\n", rf.me, upperBound, minIdx)
	for cId <= upperBound {
		if cId >= uint64(len(rf.log)) {
			log.Fatalln("out of bound")
		}
		Trace.Printf("leader %v commit %v %v", rf.me, cId, rf.log[cId])
		rf.applyCh <- ApplyMsg{int(cId), rf.log[cId].Command, false, nil}
		rf.commitIdx = cId
		rf.persist()
		cId++
	}

}


func(rf *Raft) Sync(server int) (bool, uint64) {
	//rf.pLocks[server].Lock()
	rf.mu.Lock()
	var matchedLogIdx uint64
	var entries []Entry
	if rf.nextIdx[server] == rf.matchIdx[server] + 1 {
		// consistent
		matchedLogIdx = rf.matchIdx[server]

		if matchedLogIdx + 1 < uint64(len(rf.log)){
			entries = rf.log[matchedLogIdx + 1 : ]
		}else {
			Trace.Printf("%v matched to %v, log len in master %v %v\n", server, matchedLogIdx, rf.me, len(rf.log))
		}

	}else {
		// haven't achieve consistency
		// use nextIdx and empty entries
		matchedLogIdx = rf.nextIdx[server] - 1
	}

	//rf.pLocks[server].Unlock()
	rf.mu.Unlock()

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
			Trace.Printf("%v matched become %v\n", server, matchedLogIdx)
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
				Trace.Printf("leader %v is stale, turns to follower\n", rf.me)
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
					Trace.Printf("leader %v finds a superior leader %v, turns to follower\n", rf.me, rf.votedFor)
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
