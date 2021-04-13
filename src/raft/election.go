package raft

import (
	"math/rand"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogTerm int
	LastLogIdx  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func initRandWithSeed() {
	rand.Seed(time.Now().UnixNano())
}

func getElectionTimeoutRandomly() time.Duration {
	ratio := rand.Float64() / 2
	tm := ElectionTimeoutMsBase + time.Duration(float64(ElectionTimeoutMsBase)*ratio)
	return tm
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(getElectionTimeoutRandomly())
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.persist()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
			lastLogTerm, lastLogIdx := rf.getLastLogTermAndIdx()
			if lastLogTerm < args.LastLogTerm || lastLogTerm == args.LastLogTerm && lastLogIdx <= args.LastLogIdx {
				reply.VoteGranted = true
				rf.voteFor = args.CandidateId
				return
			} else {
				reply.VoteGranted = false
				return
			}
		} else {
			reply.VoteGranted = false
			return
		}
	}

	if args.Term > rf.currentTerm {
		//收到更大的term，先更新状态；再判断日志的新旧来投票
		rf.changeToFollower(args.Term)
		//fixbug: 忘记在收到更大的term时更新votefor
		rf.voteFor = -1

		reply.Term = args.Term

		lastLogTerm, lastLogIdx := rf.getLastLogTermAndIdx()
		if lastLogTerm < args.LastLogTerm || lastLogTerm == args.LastLogTerm && lastLogIdx <= args.LastLogIdx {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
			return
		} else {
			reply.VoteGranted = false
			return
		}
	}

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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) electionHandle() {
	rf.mu.Lock()
	//fixbug: 即使是leader也需要重置自己的选举计时器
	rf.resetElectionTimer()

	//自己是leader，不用投票
	if rf.role == LEADER {
		rf.mu.Unlock()
		return
	}

	rf.changeToCandidate()

	rf.persist()

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIdx()
	request := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogTerm: lastLogTerm, LastLogIdx: lastLogIndex}

	rf.mu.Unlock()

	voteResultCh := make(chan bool, len(rf.peers))

	for peerIdx, _ := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go func(peerIdx int, request *RequestVoteArgs) {
			response := RequestVoteReply{}
			ok := rf.peers[peerIdx].Call("Raft.RequestVote", request, &response)
			if ok == false {
				voteResultCh <- false
				return
			}

			voteResultCh <- response.VoteGranted

			rf.mu.Lock()
			if response.Term > rf.currentTerm {
				rf.voteFor = -1
				rf.changeToFollower(response.Term)
				rf.persist()
			}
			rf.mu.Unlock()
		}(peerIdx, &request)
	}

	/**
	fixbug:
	1. 不能等到所有投票rpc都返回结果后才处理计票，而应该每返回一票就计算一次，
		如果支持票数大于一半or反对票大于等于一半则说明出结果了，直接处理并返回
		如果等全部RPC都返回才处理计算，则由于断开的节点迟迟无法返回，导致投票进度过慢
	2. lock的位置应该在每次读取role时，因此需要使用细粒度的锁，否则会导致死锁
	*/
	voteGrant := 1
	noGrant := 0
	for i := 1; i < len(rf.peers); i++ {
		re := <-voteResultCh
		if re == true {
			voteGrant++
		} else {
			noGrant++
		}
		rf.mu.Lock()
		if voteGrant > len(rf.peers)/2 && rf.role == CANDIDATE {
			rf.changeToLeader()
			rf.resetElectionTimer()

			rf.persist()

			rf.mu.Unlock()
			rf.heartBeatHandle()
			return
		} else if noGrant >= len(rf.peers)/2 && rf.role == CANDIDATE {
			rf.changeToFollower(rf.currentTerm)
			rf.persist()
			rf.mu.Unlock()
			return
		} else if rf.role != CANDIDATE {
			rf.persist()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

}
