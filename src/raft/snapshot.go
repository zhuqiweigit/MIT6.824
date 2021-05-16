package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotData      []byte
}

type InstallSnepshotReply struct {
	Term int
}

func (rf *Raft) Snapshot(lastApplyIndex int, lastApplyTerm int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastApplyArrayIndex := lastApplyIndex - rf.logEntryBeginIndex
	rf.logEntries = rf.logEntries[lastApplyArrayIndex:]
	rf.logEntryBeginIndex = lastApplyIndex
	rf.lastSnapshotIndex = lastApplyIndex
	rf.lastSnapshotTerm = lastApplyTerm

	raftState := rf.getRaftStateData()

	rf.persister.SaveStateAndSnapshot(raftState, snapshotData)
}

func (rf *Raft) sendSnapshot(peerIdx int) {
	rf.mu.Lock()

	request := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		SnapshotData:      rf.persister.ReadSnapshot(),
	}

	response := InstallSnepshotReply{}

	//update nextIndex
	rf.nextIndex[peerIdx] = rf.lastSnapshotIndex + 1
	rf.mu.Unlock()

	ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", &request, &response)

	if ok == false {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if response.Term > rf.currentTerm {
		rf.changeToFollower(response.Term)
		rf.voteFor = -1
		rf.resetElectionTimer()
		rf.persist()
		return
	}
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotArgs, response *InstallSnepshotReply) {
	rf.mu.Lock()

	if request.Term < rf.currentTerm {
		response.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if request.Term > rf.currentTerm {
		rf.changeToFollower(request.Term)
		rf.voteFor = -1
		rf.resetElectionTimer()
		rf.persist()
	}

	response.Term = rf.currentTerm

	if request.LastIncludedTerm < rf.lastSnapshotTerm || request.LastIncludedTerm == rf.lastSnapshotTerm && request.LastIncludedIndex < rf.lastSnapshotIndex {
		rf.mu.Unlock()
		return
	}

	rf.lastSnapshotIndex = request.LastIncludedIndex
	rf.lastSnapshotTerm = request.LastIncludedTerm

	raftState := rf.getRaftStateData()
	rf.persister.SaveStateAndSnapshot(raftState, request.SnapshotData)

	msg := ApplyMsg{
		CommandValid: false,
		Command:      rf.persister.ReadSnapshot(),
		LogIndex:     rf.lastSnapshotIndex,
		LogTerm:      rf.lastSnapshotTerm,
	}

	rf.mu.Unlock()

	rf.applyCh <- msg
	return
}

func (rf *Raft) CondInstallSnapshot(lastIncludedIndex int, lastIncludedTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rfLastLogTerm, rfLastLogIndex := rf.getLastLogTermAndIdx()
	//no need to install snapshot
	if lastIncludedTerm < rfLastLogTerm || lastIncludedTerm == rfLastLogTerm && lastIncludedIndex < rfLastLogIndex {
		return false
	}

	initEntry := LogEntry{
		Index: lastIncludedIndex,
		Term:  lastIncludedTerm,
	}
	rf.logEntries = rf.logEntries[0:0]
	rf.logEntries = append(rf.logEntries, initEntry)

	rf.logEntryBeginIndex = lastIncludedIndex
	rf.lastApplyIndex = lastIncludedIndex
	rf.commitIndex = 0

	rf.persist()
	//rf.commitIndex = lastIncludedIndex
	//no need to update rf.commitIndex

	return true
}
