package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	LogIndex     int
	LogTerm      int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type Role int

const (
	LEADER    Role = 0
	CANDIDATE Role = 1
	FOLLOWER  Role = 2
)

const (
	HeartBeatTimeoutMs    = time.Millisecond * 50
	ElectionTimeoutMsBase = time.Millisecond * 500
	ApplyTimeoutMs        = time.Millisecond * 50
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh  chan ApplyMsg
	stopCh   chan struct{}
	appendCh chan struct{}

	currentTerm int
	voteFor     int
	leaderId    int
	role        Role

	logEntries         []LogEntry
	logEntryBeginIndex int
	lastApplyIndex     int
	commitIndex        int
	nextIndex          []int
	matchIndex         []int

	lastSnapshotIndex int
	lastSnapshotTerm  int

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	applyTimer     *time.Timer
}

func (rf *Raft) changeToFollower(term int) {
	rf.role = FOLLOWER
	rf.currentTerm = term
}

func (rf *Raft) changeToCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me
}

func (rf *Raft) changeToLeader() {
	rf.role = LEADER
	rf.leaderId = rf.me
	_, lastLogIndex := rf.getLastLogTermAndIdx()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLogIndex + 1
	}
}

func (rf *Raft) PrintfLog(msg string) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	fmt.Printf("%v Id:%v\nterm:%v\nvoteFor:%v\nleaderId:%v\nrole:%v\nlastApplyIndex:%v\ncommitIndex:%v\nnext:%v\nmatch:%v\nlogLen:%v\nlastSnapshotIndex:%v\n", msg,
		rf.me, rf.currentTerm, rf.voteFor, rf.leaderId, rf.role, rf.lastApplyIndex, rf.commitIndex, rf.nextIndex, rf.matchIndex, len(rf.logEntries), rf.lastSnapshotIndex)
	//fmt.Printf("log: %v\n\n", rf.logEntries)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.role == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.logEntryBeginIndex)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getRaftStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.logEntryBeginIndex)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)

	data := w.Bytes()

	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logEntryBeginIndex int
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	var logEntries []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logEntries) != nil || d.Decode(&logEntryBeginIndex) != nil ||
		d.Decode(&lastSnapshotIndex) != nil || d.Decode(&lastSnapshotTerm) != nil {
		log.Fatal("readPersist error")
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.logEntries = logEntries
	rf.logEntryBeginIndex = logEntryBeginIndex
	rf.lastSnapshotIndex = lastSnapshotIndex
	rf.lastSnapshotTerm = lastSnapshotTerm

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	index := -1
	term := -1
	isLeader := false

	if rf.role != LEADER {
		return index, term, isLeader
	}

	_, idx := rf.getLastLogTermAndIdx()
	entry := LogEntry{Index: idx + 1, Term: rf.currentTerm, Command: command}
	rf.logEntries = append(rf.logEntries, entry)
	rf.matchIndex[rf.me] = idx + 1

	index = idx + 1
	term = rf.currentTerm
	isLeader = true
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.killed()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTimer.Stop()
	rf.heartBeatTimer.Stop()
	rf.applyTimer.Stop()

	if z == 1 {
		close(rf.stopCh)
	}
	return z == 1
}

func (rf *Raft) electionMonitorLoop() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.electionTimer.C:
			// try to elect
			rf.electionHandle()
		}
	}
}

func (rf *Raft) heartBeatSendLoop() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.heartBeatTimer.C:
			rf.heartBeatHandle()
		}
	}
}

func (rf *Raft) applyLoop() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.applyTimer.C:
			rf.applyHandle()
		}
	}
}

func (rf *Raft) resetApplyTimer() {

	if !rf.applyTimer.Stop() {
		select {
		case <-rf.applyTimer.C:
		default:
		}
	}
	rf.applyTimer.Reset(ApplyTimeoutMs)
}

func (rf *Raft) applyHandle() {
	rf.mu.Lock()

	rf.resetApplyTimer()

	msgBuffer := make([]ApplyMsg, 0)

	if rf.lastApplyIndex < rf.commitIndex {
		for i := rf.lastApplyIndex + 1; i <= rf.commitIndex; i++ {
			idx := i - rf.logEntryBeginIndex
			//fmt.Printf("rf:%v i:%v  begin:%v  idx:%v  apply:%v  commit:%v\n", rf.me, i, rf.logEntryBeginIndex, idx, rf.lastApplyIndex, rf.commitIndex)
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				LogIndex:     rf.logEntries[idx].Index,
				LogTerm:      rf.logEntries[idx].Term,
				Command:      rf.logEntries[idx].Command}
			msgBuffer = append(msgBuffer, applyMsg)
		}
		//rf.lastApplyIndex = rf.commitIndex
	}

	rf.mu.Unlock()

	//fixbug: channel不能加锁
	for idx, _ := range msgBuffer {
		rf.applyCh <- msgBuffer[idx]
		rf.mu.Lock()
		rf.lastApplyIndex++
		rf.mu.Unlock()
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

func (rf *Raft) getLastLogTermAndIdx() (int, int) {
	len := len(rf.logEntries)
	return rf.logEntries[len-1].Term, rf.logEntries[len-1].Index
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	initRandWithSeed()

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.stopCh = make(chan struct{})
	rf.appendCh = make(chan struct{})

	rf.currentTerm = 0
	rf.voteFor = -1
	rf.leaderId = -1
	rf.role = FOLLOWER

	initLog := LogEntry{Index: 0, Term: 0}
	rf.logEntries = append(rf.logEntries, initLog)
	rf.logEntryBeginIndex = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastApplyIndex = rf.lastSnapshotIndex

	rf.heartBeatTimer = time.NewTimer(HeartBeatTimeoutMs)
	rf.electionTimer = time.NewTimer(getElectionTimeoutRandomly())
	rf.applyTimer = time.NewTimer(ApplyTimeoutMs)

	go rf.applyLoop()
	go rf.electionMonitorLoop()
	go rf.heartBeatSendLoop()

	return rf
}
