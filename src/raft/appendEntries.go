package raft

import (
	"sort"
)

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
	if !rf.heartBeatTimer.Stop() {
		select {
		case <-rf.heartBeatTimer.C:
		default:
		}
	}
	rf.heartBeatTimer.Reset(HeartBeatTimeoutMs)
}

func (rf *Raft) AppendEntry(request *RequestAppendEntry, response *ResponseAppendEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if request.Term < rf.currentTerm {
		response.Success = false
		response.Term = rf.currentTerm
		return
	}

	if request.Term > rf.currentTerm {
		rf.voteFor = -1
	}
	rf.changeToFollower(request.Term)
	rf.resetElectionTimer()
	rf.leaderId = request.LeaderId

	//换算为实际数组的index
	prevLogIdx := request.PrevLogIdx - rf.logEntryBeginIndex
	if prevLogIdx >= len(rf.logEntries) {
		response.Success = false
		response.Term = rf.currentTerm
		return
	} else {
		//不匹配
		if rf.logEntries[prevLogIdx].Term != request.PrevLogTerm {
			response.Success = false
			response.Term = rf.currentTerm
			return
		}
		/**
		fixBug1:
			如果prevLogIdx处匹配成功了，代表prevLogIdx之前的全部匹配成功，但不代表之后的也匹配上了
			因此需要在[0:prevLogIdx+1]的基础上append，而非直接简单地追加在整个rf.logEntries的最后面
		fixBug2:
			考虑leader重复发送append log的情况，先发[1,2]，后发[1,2,3,4]，但由于网络延迟，后发的先到，先发的后到，则短的不能覆盖长的
		fixBug3:
			在prev匹配上的基础上，如果leader发送的append log较短，而follower的log[prev+1: ]较长，则需要从prevIdx+1开始一个一个判断是否有冲突。
			如果有冲突，则把冲突部分到尾部的全部丢弃，再把append log的剩余部分附加上去；
			如果没有冲突，则说明不需要append log
		*/
		tailLen := len(rf.logEntries) - prevLogIdx - 1
		requestLogLen := len(request.Entries)
		if tailLen <= requestLogLen {
			rf.logEntries = append(rf.logEntries[0:prevLogIdx+1], request.Entries...)
		} else {
			rfPtr := prevLogIdx + 1
			reqPtr := 0
			for j := 0; j < len(request.Entries); j++ {
				if rf.logEntries[rfPtr].Term != request.Entries[reqPtr].Term {
					rf.logEntries = rf.logEntries[0:rfPtr]
					break
				}
				rfPtr++
				reqPtr++
			}
			rf.logEntries = append(rf.logEntries, request.Entries[reqPtr:]...)
		}

		if rf.commitIndex < request.LeaderCommit {
			rf.commitIndex = min(request.LeaderCommit, rf.logEntries[len(rf.logEntries)-1].Index)
		}
		response.Success = true
		response.Term = rf.currentTerm
		return
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) heartBeatHandle() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetHeartBeatTimer()

	if rf.role != LEADER {
		return
	}

	rf.tryToUpdateCommitIndex()

	//send entries
	for peerIdx, _ := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		if rf.role != LEADER {
			return
		}

		go func(peerIdx int) {
			rf.mu.Lock()
			request := rf.makeAppendedEntries(peerIdx)
			rf.mu.Unlock()

			response := ResponseAppendEntry{}
			ok := rf.peers[peerIdx].Call("Raft.AppendEntry", &request, &response)
			if ok == false {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			defer rf.persist()

			if response.Term > rf.currentTerm {
				rf.voteFor = -1
				rf.changeToFollower(response.Term)
				return
			}
			if rf.role != LEADER || rf.currentTerm != request.Term || response.Term < rf.currentTerm {
				return
			}
			//匹配失败
			if response.Success == false {
				failMatchTerm := request.PrevLogTerm
				failMatchIndex := request.PrevLogIdx
				var arrayIndex int
				for arrayIndex = failMatchIndex - rf.logEntryBeginIndex; arrayIndex >= 0 && rf.logEntries[arrayIndex].Term == failMatchTerm; arrayIndex-- {
					rf.nextIndex[peerIdx] = rf.logEntries[arrayIndex].Index
				}

			} else {
				//匹配成功
				sendLen := len(request.Entries)
				if sendLen != 0 {
					//fixBug: 考虑到网络延迟，有可能matchIndex比较小的reply过了很久才发送过来，此时要避免它把当前matchIndex缩短
					if rf.matchIndex[peerIdx] < request.Entries[sendLen-1].Index {
						rf.matchIndex[peerIdx] = request.Entries[sendLen-1].Index
					}
					if rf.nextIndex[peerIdx] < rf.matchIndex[peerIdx]+1 {
						rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1
					}
				}
			}
		}(peerIdx)
	}
}

func (rf *Raft) tryToUpdateCommitIndex() {
	tempMatch := make([]int, len(rf.matchIndex))
	copy(tempMatch, rf.matchIndex)
	sort.Ints(tempMatch)
	midIdx := (len(rf.matchIndex) - 1) / 2
	N := tempMatch[midIdx]
	logIndex := N - rf.logEntryBeginIndex

	if N > rf.commitIndex && rf.logEntries[logIndex].Term == rf.currentTerm {
		rf.commitIndex = N
	}

}

func (rf *Raft) makeAppendedEntries(peerIndex int) RequestAppendEntry {
	next := rf.nextIndex[peerIndex]
	arrayIdx := next - rf.logEntryBeginIndex
	logs := append([]LogEntry{}, rf.logEntries[arrayIdx:]...)

	request := RequestAppendEntry{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIdx:   rf.logEntries[arrayIdx-1].Index,
		PrevLogTerm:  rf.logEntries[arrayIdx-1].Term,
		LeaderCommit: rf.commitIndex,
		//fixbug: slice不是深拷贝，仅仅是引用。因此这里应该调用copy得到log，而非rf.logEntries[arrayIdx:]；
		//否则会发生race
		Entries: logs,
	}

	return request
}
