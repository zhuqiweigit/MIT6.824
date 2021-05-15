package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const (
	RaftApplyTimeout           = time.Millisecond * 1000
	PullConfigTimeout          = time.Millisecond * 250
	FetchShardsTimeout         = time.Millisecond * 100
	SendDelShardsSignalTimeout = time.Millisecond * 100
)

const (
	NoAction  = 0
	NeedFetch = 1
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	OperationId int64
	Key         string
	Value       string
	Method      string
}

type NotifyMsg struct {
	Err   Err
	Value string
}

type ShardData struct {
	ShardId         int
	ConfigNum       int
	Data            map[string]string //key-value
	RecentApplyOpId map[int64]int64   //clientId-opId
}

func DupShard(shd ShardData) ShardData {
	newShardData := ShardData{ShardId: shd.ShardId, ConfigNum: shd.ConfigNum}
	newShardData.Data = make(map[string]string)
	newShardData.RecentApplyOpId = make(map[int64]int64)
	for key, val := range shd.Data {
		newShardData.Data[key] = val
	}
	for clientId, opId := range shd.RecentApplyOpId {
		newShardData.RecentApplyOpId[clientId] = opId
	}
	return newShardData
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	masters  []*labrpc.ClientEnd
	dead     int32

	shardmasterClient *shardmaster.Clerk

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	dataBase            map[int]ShardData //shardId-shardData
	needShards          []int
	waitToSendDelSignal map[string]shardmaster.Config //{configNum:shardId}-config in that time
	discardShards       map[string]ShardData          //{configNum:shardId}-shardData

	lastApplyIndex int
	lastApplyTerm  int

	config    shardmaster.Config
	oldconfig shardmaster.Config

	applyNotify map[string]chan NotifyMsg //notifyChs map[string]chan
	stopCh      chan struct{}
	delNotify   map[string]chan DelShardsRequest //map["gid:shardId"]chan

	pullConfigTimer          *time.Timer
	fetchShardsTimer         *time.Timer
	sendDelShardsSignalTimer *time.Timer
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{ClientId: args.ClientId, OperationId: args.OperationId, Key: args.Key, Method: "Get"}

	msg := kv.startToRaftAndWait(op)

	reply.Err = msg.Err
	reply.Value = msg.Value

	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{ClientId: args.ClientId, OperationId: args.OperationId, Key: args.Key, Value: args.Value, Method: args.Op}

	msg := kv.startToRaftAndWait(op)

	reply.Err = msg.Err

	return
}

func (kv *ShardKV) startToRaftAndWait(op Op) NotifyMsg {
	kv.mu.Lock()
	msg := NotifyMsg{}

	shardId := key2shard(op.Key)
	if _, ok := kv.dataBase[shardId]; ok == false {
		msg.Err = ErrWrongGroup
		kv.mu.Unlock()
		return msg
	}

	shard := kv.dataBase[shardId]
	if _, ok := shard.RecentApplyOpId[op.ClientId]; ok == false {
		shard.RecentApplyOpId[op.ClientId] = -1
	}
	if op.OperationId < shard.RecentApplyOpId[op.ClientId] {
		msg.Err = ErrOldOperation
		kv.mu.Unlock()
		return msg
	}

	notifyCh := make(chan NotifyMsg, 1)
	clientOpPair := fmt.Sprintf("%v:%v", op.ClientId, op.OperationId)
	kv.applyNotify[clientOpPair] = notifyCh

	kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		msg.Err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.applyNotify, clientOpPair)
		kv.mu.Unlock()
		return msg
	}

	timer := time.NewTimer(RaftApplyTimeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		{
			msg.Err = ErrRaftTimeout
			kv.mu.Lock()
			delete(kv.applyNotify, clientOpPair)
			kv.mu.Unlock()
			return msg
		}
	case msg = <-notifyCh:
		{
			return msg
		}
	}
}

func (kv *ShardKV) applyToStateMachineLoop() {
	for {
		select {
		case <-kv.stopCh:
			{
				return
			}
		case raftApplyMsg := <-kv.applyCh:
			{
				if raftApplyMsg.CommandValid == false {
					//do snapshot
					snapshotData := raftApplyMsg.Command.([]byte)
					needInstall := kv.rf.CondInstallSnapshot(raftApplyMsg.LogIndex, raftApplyMsg.LogTerm)
					if needInstall == false {
						break
					}
					kv.recoverKVServerSnapshot(snapshotData)
				} else {
					if op, ok := raftApplyMsg.Command.(Op); ok == true {
						kv.executeOperation(op, raftApplyMsg)
					} else if config, ok := raftApplyMsg.Command.(shardmaster.Config); ok == true {
						kv.executeConfig(config, raftApplyMsg)
					} else if shardData, ok := raftApplyMsg.Command.(ShardData); ok == true {
						kv.executeAddedShard(shardData, raftApplyMsg)
					} else if delShardRequest, ok := raftApplyMsg.Command.(DelShardsRequest); ok == true {
						kv.executeDelShard(delShardRequest, raftApplyMsg)
					} else if configNumShardIdPair, ok := raftApplyMsg.Command.(string); ok == true {
						kv.executeDelWaitToSendDelSignalList(configNumShardIdPair, raftApplyMsg)
					}
				}
				kv.checkIfNeedShapshot()
			}
		}
	}
}

func (kv *ShardKV) pullConfigLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullConfigTimer.C:
			kv.pullConfigAndStart()
		}
	}
}

func (kv *ShardKV) fetchShardsLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.fetchShardsTimer.C:
			kv.fetchShards()
		}
	}
}

func (kv *ShardKV) sendDelShardsSignalLoop() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.sendDelShardsSignalTimer.C:
			kv.sendDelShardsSignal()
		}
	}
}

func (kv *ShardKV) checkIfNeedShapshot() {
	if kv.maxraftstate == -1 {
		return
	}

	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)

		kv.mu.Lock()
		lastApplyIndex := kv.lastApplyIndex
		lastApplyTerm := kv.lastApplyTerm

		e.Encode(kv.lastApplyIndex)
		e.Encode(kv.lastApplyTerm)
		e.Encode(kv.dataBase)
		e.Encode(kv.needShards)
		e.Encode(kv.waitToSendDelSignal)
		e.Encode(kv.discardShards)
		e.Encode(kv.config)
		e.Encode(kv.oldconfig)
		kv.mu.Unlock()

		snapshotData := w.Bytes()

		kv.rf.Snapshot(lastApplyIndex, lastApplyTerm, snapshotData)
	} else {
		return
	}
}

func (kv *ShardKV) recoverKVServerSnapshot(snapshotData []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if snapshotData == nil || len(snapshotData) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)

	var lastApplyIndex int
	var lastApplyTerm int
	var dataBase map[int]ShardData
	var needShards []int
	var waitToSendDelSignal map[string]shardmaster.Config
	var discardShards map[string]ShardData
	var config shardmaster.Config
	var oldconfig shardmaster.Config

	if d.Decode(&lastApplyIndex) != nil || d.Decode(&lastApplyTerm) != nil ||
		d.Decode(&dataBase) != nil || d.Decode(&needShards) != nil || d.Decode(&waitToSendDelSignal) != nil || d.Decode(&discardShards) != nil ||
		d.Decode(&config) != nil || d.Decode(&oldconfig) != nil {
		log.Fatal("decode snapshot error")
	}

	kv.lastApplyIndex = lastApplyIndex
	kv.lastApplyTerm = lastApplyTerm
	kv.dataBase = dataBase
	kv.needShards = needShards
	kv.waitToSendDelSignal = waitToSendDelSignal
	kv.discardShards = discardShards
	kv.config = config
	kv.oldconfig = oldconfig
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killed()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	if z == 1 {
		close(kv.stopCh)
	}
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

/**
1. servers: shardKV这个group的所有节点
2. masters: shardmaster整个group的所有节点；应该用这个数据传递给shardmaster的client，从而制作出一个shardmaster的client。以后查询config数据应该通过这个client，而非手动设计RPC调用
3. make_end: 通过节点的string名字得到通信对象。可以用于借助config得到其它shardKV节点的name后，与其通信(主要是交换shard分片数据)
*/
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(FetchShardsRequest{})
	labgob.Register(DelShardsRequest{})
	labgob.Register(ShardData{})
	labgob.Register(shardmaster.Config{})
	//labgob.Register(string{})

	kv.shardmasterClient = shardmaster.MakeClerk(masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.dataBase = make(map[int]ShardData)
	kv.needShards = make([]int, shardmaster.NShards)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.needShards[i] = NoAction
	}
	kv.waitToSendDelSignal = make(map[string]shardmaster.Config)
	kv.discardShards = make(map[string]ShardData)

	kv.config = shardmaster.Config{Num: 0, Groups: make(map[int][]string)}
	kv.oldconfig = shardmaster.Config{Num: 0, Groups: make(map[int][]string)}

	kv.applyNotify = make(map[string]chan NotifyMsg)
	kv.delNotify = make(map[string]chan DelShardsRequest)
	kv.stopCh = make(chan struct{})

	kv.pullConfigTimer = time.NewTimer(PullConfigTimeout)
	kv.fetchShardsTimer = time.NewTimer(FetchShardsTimeout)
	kv.sendDelShardsSignalTimer = time.NewTimer(SendDelShardsSignalTimeout)

	kv.recoverKVServerSnapshot(persister.ReadSnapshot())

	go kv.pullConfigLoop()
	go kv.applyToStateMachineLoop()
	go kv.fetchShardsLoop()
	go kv.sendDelShardsSignalLoop()

	return kv
}
