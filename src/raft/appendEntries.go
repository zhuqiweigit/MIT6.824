package raft

type RequestAppendEntry struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type ResponseAppendEntry struct {
	Term    int
	Success bool
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.heartBeatTimer.Stop()
	rf.heartBeatTimer.Reset(HeartBeatTimeoutMs)
}

func (rf *Raft) AppendEntry(request *RequestAppendEntry, response *ResponseAppendEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == CANDIDATE {
		rf.role = FOLLOWER
	}

	if request.Term < rf.currentTerm {
		response.Success = false
		response.Term = rf.currentTerm
		return
	} else if request.Term == rf.currentTerm {
		response.Success = true
		rf.resetElectionTimer()
		return
	} else {
		if rf.role == LEADER {
			rf.role = FOLLOWER
			rf.voteFor = -1
			rf.currentTerm = request.Term
		}
		rf.leaderId = request.LeaderId
		response.Success = true
		response.Term = request.Term
		rf.resetElectionTimer()
		return
	}
}

func (rf *Raft) heartBeatHandle() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.resetHeartBeatTimer()

	if rf.role != LEADER {
		return
	}

	request := RequestAppendEntry{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIdx:   1,
		PrevLogTerm:  1,
		LeaderCommit: 1,
	}

	for peerIdx, _ := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go func(peerIdx int, request *RequestAppendEntry) {
			response := ResponseAppendEntry{}
			ok := rf.peers[peerIdx].Call("Raft.AppendEntry", request, &response)
			if ok == false {
				return
			}
			/*
				if response.Term > rf.currentTerm {
					rf.role = FOLLOWER
					rf.voteFor = -1
					rf.currentTerm = request.Term
				}*/
		}(peerIdx, &request)
	}
}
