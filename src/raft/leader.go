package raft

import (
	"log"
	"time"
)


func(rf *Raft) leaderCommit() {
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
		}else {
			rf.logger.Error.Fatal("get term %v > current term %v\n", rf.log[i].Term, rf.currentTerm)
		}
	}

	if minIdx == 0 {
		// can't find entry in current term
		// unsafe to commit
		return
	}

	minIdx += int(rf.startIdx)

	// find the safe upper bound
	upperBound := rf.commitIdx
	for minIdx < len(rf.log) + int(rf.startIdx)  {
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
	//rf.logger.Trace.Printf("leader %v upperbound %v min %v\n", rf.me, upperBound, minIdx)
	for cId <= upperBound {
		if cId >= uint64(len(rf.log)) + rf.startIdx {
			rf.logger.Error.Fatalln("out of bound")
		}
		rf.logger.Trace.Printf("leader %v commit %v %v", rf.me, cId, rf.log[cId - rf.startIdx])
		rf.applyCh <- ApplyMsg{int(cId), rf.log[cId - rf.startIdx].Command, false, nil}
		rf.commitIdx = cId
		rf.persist()
		cId++
	}

}


func(rf *Raft) sync(server int) (bool, uint64) {
	rf.mu.Lock()
	if server >= len(rf.nextIdx) {
		rf.logger.Error.Printf("invalid mm %v %v\n", server, len(rf.nextIdx))
	}
	if server >= len(rf.matchIdx) {
		rf.logger.Error.Printf("invalid mm %v %v\n", server, len(rf.matchIdx))
	}

	var matchedLogIdx uint64
	var matchedTerm uint64
	var entries []Entry = nil

	var snapshot []byte = nil
	if rf.nextIdx[server] - 1 < rf.startIdx {
		// a slow follower, send snapshot
		matchedLogIdx = rf.startIdx
		snapshot = rf.persister.ReadSnapshot()

		entries = rf.log
	}else if rf.nextIdx[server] - 1 == rf.startIdx {
		matchedLogIdx = rf.startIdx
		matchedTerm = rf.startTerm

		entries = rf.log[matchedLogIdx - rf.startIdx + 1: ]
	}else if rf.nextIdx[server] == rf.matchIdx[server] + 1 {
		// consistent
		matchedLogIdx = rf.matchIdx[server]

		if matchedLogIdx + 1 < uint64(len(rf.log)) + rf.startIdx{

		}else {
			rf.logger.Trace.Printf("%v matched to %v, log len in master %v %v\n", server, matchedLogIdx, rf.me, len(rf.log))
		}
		entries = rf.log[matchedLogIdx - rf.startIdx + 1 : ]
		matchedTerm = rf.log[matchedLogIdx - rf.startIdx].Term
	}else {
		// haven't achieve consistency, but follower is up-to-date
		matchedLogIdx = rf.nextIdx[server] - 1

		entries = rf.log[matchedLogIdx - rf.startIdx + 1 : ]
		matchedTerm = rf.log[matchedLogIdx - rf.startIdx].Term
	}
	rf.mu.Unlock()

	args := AppendEntriesArgs{rf.currentTerm, rf.me, matchedLogIdx, matchedTerm, entries, rf.commitIdx, snapshot}
	rf.logger.Trace.Printf("leader %v send %v to %v\n", rf.me, args, server)
	reply := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(server, args, reply)

	if !ok {

		return false, 0
	}

	rf.mu.Lock()
	if reply.Success {
		matchedLogIdx = matchedLogIdx + uint64(len(entries))
		if rf.matchIdx[server] < matchedLogIdx{
			rf.logger.Trace.Printf("%v matched become %v, leader is %v\n", server, matchedLogIdx, rf.me)
			rf.matchIdx[server] = matchedLogIdx
			rf.nextIdx[server] = matchedLogIdx + 1
		}
	} else if reply.Term == rf.currentTerm {
		if reply.CommitId > uint64(len(rf.log)) + rf.startIdx {
			rf.logger.Error.Fatalln("follower commit more than leader")
		}
		rf.matchIdx[server] = reply.CommitId
		rf.nextIdx[server] = reply.CommitId + 1
	}

	rf.mu.Unlock()
	if reply.Success {
		rf.leaderCommit()
	}

	return true, reply.Term
}

// used by leader to send out heartbeat
func (rf *Raft) broadcastHeartBeat() {
	waitTime := time.Duration(HEARTBEATINTERVAL)
	timmer := time.NewTimer(waitTime * time.Millisecond)
	for {
		if rf.role != LEADER {
			log.Fatalf("call broadcast heartbeat, but I'm not a leader\n")
		}

		// send out heartheat every HEARTBEATINTERVAL ms

		staleSignal := make(chan bool, len(rf.peers) - 1)

		// broadcast heartheat in parallel
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				// escape myself
				continue
			}
			go func(server int) {
				ok, term := rf.sync(server)
				if ok && term > rf.currentTerm {
					staleSignal <- true
				}
			}(i)

		}

		endLoop := false
		for !endLoop{
			select {
			case <-rf.kill:
				return;
			case <-staleSignal:
			// my Term is stale
			// convert to follower stage
				rf.mu.Lock()
				rf.role = FOLLOWER
			// rf.nextIdx = nil
			// rf.matchIdx = nil
				rf.mu.Unlock()
				rf.logger.Trace.Printf("leader %v is stale, turns to follower\n", rf.me)
				go rf.heartBeatTimer()
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
					rf.logger.Trace.Printf("leader %v finds a superior leader %v, turns to follower\n", rf.me, rf.votedFor)
					go rf.heartBeatTimer()
					return
				}

			case <- timmer.C:
			// begin another broadcast round
				endLoop = true
				timmer.Reset(waitTime * time.Millisecond)
				break
			}
		}
	}
}
