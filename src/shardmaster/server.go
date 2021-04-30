package shardmaster

import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "time"
import "fmt"
import "bytes"
import "log"

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)
const (
	RaftTimeOutMs = time.Millisecond * 2000
)

type NotifyMsg struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftStateSize int
	persister        *raft.Persister

	configs []Config // indexed by config num

	// Your data here.
	notifyChs     map[string]chan NotifyMsg
	lastApplyOpId map[int64]int

	lastApplyRaftTerm   int
	lastApplyRaftLogIdx int

	stopCh chan struct{}
}

type Op struct {
	// Your data here.
	ClientId    int64
	OperationId int
	OpType      string
	OpCmd       interface{}
}

func (sm *ShardMaster) PrintLog() {
	fmt.Printf("meId: %v %+v\n", sm.me, sm.configs)
}
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	myArg := JoinArgs{OperationId: args.OperationId, ClientId: args.ClientId}
	myArg.Servers = make(map[int][]string)
	for gid, servers := range args.Servers {
		myArg.Servers[gid] = make([]string, len(servers))
		copy(myArg.Servers[gid], servers)
	}
	op := Op{OpType: Join, OpCmd: myArg, ClientId: args.ClientId, OperationId: args.OperationId}
	msg := sm.startToRaftAndWait(op)
	reply.Err = msg.Err
	reply.WrongLeader = msg.WrongLeader
	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	myArg := LeaveArgs{OperationId: args.OperationId, ClientId: args.ClientId}
	myArg.GIDs = make([]int, len(args.GIDs))
	copy(myArg.GIDs, args.GIDs)

	op := Op{OpType: Leave, OpCmd: myArg, ClientId: args.ClientId, OperationId: args.OperationId}
	msg := sm.startToRaftAndWait(op)
	reply.Err = msg.Err
	reply.WrongLeader = msg.WrongLeader
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	myArg := MoveArgs{Shard: args.Shard, GID: args.GID, OperationId: args.OperationId, ClientId: args.ClientId}

	op := Op{OpType: Move, OpCmd: myArg, ClientId: args.ClientId, OperationId: args.OperationId}
	msg := sm.startToRaftAndWait(op)
	reply.Err = msg.Err
	reply.WrongLeader = msg.WrongLeader
	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	myArg := QueryArgs{Num: args.Num, OperationId: args.OperationId, ClientId: args.ClientId}

	op := Op{OpType: Query, OpCmd: myArg, ClientId: args.ClientId, OperationId: args.OperationId}
	msg := sm.startToRaftAndWait(op)
	reply.Err = msg.Err
	reply.WrongLeader = msg.WrongLeader
	reply.Config = msg.Config
	//sm.PrintLog()
	return
}

func (sm *ShardMaster) startToRaftAndWait(op Op) NotifyMsg {
	sm.mu.Lock()

	if _, ok := sm.lastApplyOpId[op.ClientId]; ok == false {
		sm.lastApplyOpId[op.ClientId] = -1
	}

	if op.OperationId < sm.lastApplyOpId[op.ClientId] {
		msg := NotifyMsg{Err: OperationIdOld}
		sm.mu.Unlock()
		return msg
	}

	notifyCh := make(chan NotifyMsg)
	key := fmt.Sprintf("%v:%v", op.ClientId, op.OperationId)
	sm.notifyChs[key] = notifyCh
	sm.mu.Unlock()

	_, _, isLeader := sm.rf.Start(op)
	if isLeader == false {
		msg := NotifyMsg{WrongLeader: true}
		sm.mu.Lock()
		delete(sm.notifyChs, key)
		sm.mu.Unlock()
		return msg
	}

	timer := time.NewTimer(RaftTimeOutMs)
	select {
	case msg := <-notifyCh:
		{
			msg.WrongLeader = false
			return msg
		}
	case <-timer.C:
		{
			msg := NotifyMsg{WrongLeader: false, Err: RaftApplyTimeout}
			return msg
		}
	case <-sm.stopCh:
		{
			return NotifyMsg{}
		}
	}
}

func (sm *ShardMaster) applyToStateMachineLoop() {
	for {
		select {
		case <-sm.stopCh:
			{
				return
			}
		case raftApplyMsg := <-sm.applyCh:
			{
				if raftApplyMsg.CommandValid == false {
					lastIncludedIdx := raftApplyMsg.LogIndex
					lastIncludedTerm := raftApplyMsg.LogTerm
					if sm.rf.CondInstallSnapshot(lastIncludedIdx, lastIncludedTerm) == true {
						snapshotData := raftApplyMsg.Command.([]byte)
						sm.recoverShardMasterFromSnapShot(snapshotData)
					}
				} else {
					op := raftApplyMsg.Command.(Op)
					sm.executeOperation(op, raftApplyMsg)
					//fmt.Printf("meId %v \nexec %+v\n", sm.me, op)
					//sm.PrintLog()
				}
				sm.checkIfNeedSnapshot()
			}
		}
	}
}

func (sm *ShardMaster) executeOperation(op Op, raftApplyMsg raft.ApplyMsg) {
	sm.mu.Lock()

	key := fmt.Sprintf("%v:%v", op.ClientId, op.OperationId)

	if _, ok := sm.lastApplyOpId[op.ClientId]; ok == false {
		sm.lastApplyOpId[op.ClientId] = -1
	}

	msg := NotifyMsg{}
	if op.OperationId < sm.lastApplyOpId[op.ClientId] {
		msg = NotifyMsg{WrongLeader: false, Err: OperationIdOld}
	} else if op.OperationId == sm.lastApplyOpId[op.ClientId] {
		msg = NotifyMsg{WrongLeader: false, Err: OK}
		if op.OpType == Query {
			queryArg := op.OpCmd.(QueryArgs)
			msg.Config = sm.query(queryArg.Num)
		}
	} else {
		sm.lastApplyRaftLogIdx = raftApplyMsg.LogIndex
		sm.lastApplyRaftTerm = raftApplyMsg.LogTerm

		sm.lastApplyOpId[op.ClientId] = op.OperationId

		msg = NotifyMsg{WrongLeader: false, Err: OK}

		if op.OpType == Join {
			joinArg := op.OpCmd.(JoinArgs)
			sm.joinGroups(joinArg.Servers)
		} else if op.OpType == Leave {
			leaveArg := op.OpCmd.(LeaveArgs)
			sm.leaveGroups(leaveArg.GIDs)
		} else if op.OpType == Move {
			moveArg := op.OpCmd.(MoveArgs)
			sm.moveGroup(moveArg.GID, moveArg.Shard)
		} else if op.OpType == Query {
			queryArg := op.OpCmd.(QueryArgs)
			msg.Config = sm.query(queryArg.Num)
		}
	}

	if _, ok := sm.notifyChs[key]; ok == false {
		sm.mu.Unlock()
		return
	}
	notifyCh := sm.notifyChs[key]
	delete(sm.notifyChs, key)
	sm.mu.Unlock()

	notifyCh <- msg
}

func (sm *ShardMaster) checkIfNeedSnapshot() {
	if sm.maxRaftStateSize == -1 {
		return
	}
	if sm.persister.RaftStateSize() > sm.maxRaftStateSize {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		sm.mu.Lock()
		lastApplyTerm := sm.lastApplyRaftTerm
		lastApplyLogIdx := sm.lastApplyRaftLogIdx
		e.Encode(sm.lastApplyRaftTerm)
		e.Encode(sm.lastApplyRaftLogIdx)
		e.Encode(sm.configs)
		e.Encode(sm.lastApplyOpId)
		sm.mu.Unlock()

		snapShotData := w.Bytes()

		sm.rf.Snapshot(lastApplyLogIdx, lastApplyTerm, snapShotData)

	}
}

func (sm *ShardMaster) recoverShardMasterFromSnapShot(snapshotData []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if snapshotData == nil || len(snapshotData) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)

	var lastApplyTerm int
	var lastApplyLogIdx int
	var configs []Config
	var lastApplyOpId map[int64]int
	if d.Decode(&lastApplyTerm) != nil || d.Decode(&lastApplyLogIdx) != nil || d.Decode(&configs) != nil || d.Decode(&lastApplyOpId) != nil {
		log.Fatal("decode snapshot fail")
	}

	sm.lastApplyRaftTerm = lastApplyTerm
	sm.lastApplyRaftLogIdx = lastApplyLogIdx
	sm.configs = configs
	sm.lastApplyOpId = lastApplyOpId
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.stopCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configs[0].Num = 0
	for i := 0; i < NShards; i++ {
		sm.configs[0].Shards[i] = 0
	}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.persister = persister

	sm.stopCh = make(chan struct{})

	sm.maxRaftStateSize = 10240000
	sm.notifyChs = make(map[string]chan NotifyMsg)
	sm.lastApplyOpId = make(map[int64]int)
	sm.lastApplyRaftTerm = 0
	sm.lastApplyRaftLogIdx = 0

	sm.recoverShardMasterFromSnapShot(persister.ReadSnapshot())

	go sm.applyToStateMachineLoop()

	return sm
}
