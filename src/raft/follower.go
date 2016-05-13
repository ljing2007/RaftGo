package raft

import (
	"time"
	"log"
)


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
