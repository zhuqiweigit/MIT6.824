package kvraft

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
)

const Debug = 0
const RaftApplyTimeout = time.Millisecond * 1000

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ClientId    int64
	OperationId int64
	Key         string
	Value       string
	Method      string
}

type ApplyMsg struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	lastApplyIndex int
	lastApplyTerm  int

	recentApplyOpId map[int64]int64          //map[clientId]opId
	applyNotify     map[string]chan ApplyMsg //map[clientId+opId]chan

	stopCh chan struct{}

	dataBase map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{ClientId: args.ClientId, OperationId: args.OperationId, Key: args.Key, Method: "Get"}

	msg := kv.operationHandle(op)

	reply.Err = msg.Err
	reply.Value = msg.Value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{ClientId: args.ClientId, OperationId: args.OperationId, Key: args.Key, Value: args.Value, Method: args.Op}

	msg := kv.operationHandle(op)

	reply.Err = msg.Err

	return
}

func (kv *KVServer) operationHandle(op Op) ApplyMsg {
	kv.mu.Lock()

	msg := ApplyMsg{}

	if _, ok := kv.recentApplyOpId[op.ClientId]; ok == false {
		kv.recentApplyOpId[op.ClientId] = -1
	}

	if op.OperationId < kv.recentApplyOpId[op.ClientId] {
		msg.Err = ErrOldOperation
		kv.mu.Unlock()
		return msg
	}

	ch := make(chan ApplyMsg, 1)
	clientOpPair := fmt.Sprintf("%v:%v", op.ClientId, op.OperationId)
	kv.applyNotify[clientOpPair] = ch

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
	case msg = <-ch:
		{
			return msg
		}
	}

}

func (kv *KVServer) applyOperationToDataBase(msg raft.ApplyMsg, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	result := ApplyMsg{}

	clientOpPair := fmt.Sprintf("%v:%v", op.ClientId, op.OperationId)

	if _, ok := kv.recentApplyOpId[op.ClientId]; ok == false {
		kv.recentApplyOpId[op.ClientId] = -1
	}

	if op.OperationId < kv.recentApplyOpId[op.ClientId] {
		result.Err = ErrOldOperation
	} else if op.OperationId == kv.recentApplyOpId[op.ClientId] {
		kv.lastApplyIndex = msg.LogIndex
		kv.lastApplyTerm = msg.LogTerm

		if op.Method == "Get" {
			val, ok := kv.dataBase[op.Key]
			if ok == false {
				result.Err = ErrNoKey
				result.Value = ""
			} else {
				result.Err = OK
				result.Value = val
			}
		} else {
			result.Err = OK
		}
	} else if op.OperationId > kv.recentApplyOpId[op.ClientId] {
		kv.lastApplyIndex = msg.LogIndex
		kv.lastApplyTerm = msg.LogTerm

		if op.Method == "Get" {
			val, ok := kv.dataBase[op.Key]
			if ok == false {
				result.Err = ErrNoKey
				result.Value = ""
			} else {
				result.Err = OK
				result.Value = val
			}
		} else if op.Method == "Put" {
			kv.dataBase[op.Key] = op.Value
			result.Err = OK
		} else if op.Method == "Append" {
			val, ok := kv.dataBase[op.Key]
			if ok == false {
				kv.dataBase[op.Key] = op.Value
			} else {
				val = val + op.Value
				kv.dataBase[op.Key] = val
			}
			result.Err = OK
		}
		kv.recentApplyOpId[op.ClientId] = op.OperationId
	}

	if ch, ok := kv.applyNotify[clientOpPair]; ok == true {
		ch <- result
		delete(kv.applyNotify, clientOpPair)
	}

}

//已加锁，注意防止死锁
func (kv *KVServer) checkIfNeedSnapshot() {

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
		e.Encode(kv.recentApplyOpId)
		kv.mu.Unlock()

		snapshotData := w.Bytes()

		kv.rf.Snapshot(lastApplyIndex, lastApplyTerm, snapshotData)

	} else {
		return
	}
}

func (kv *KVServer) recoverKVServerSnapshot(snapshotData []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if snapshotData == nil || len(snapshotData) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)

	var lastApplyIndex int
	var lastApplyTerm int
	var dataBase map[string]string
	var recentApplyOpId map[int64]int64

	if d.Decode(&lastApplyIndex) != nil || d.Decode(&lastApplyTerm) != nil || d.Decode(&dataBase) != nil || d.Decode(&recentApplyOpId) != nil {
		log.Fatal("decode snapshot error")
	}

	kv.lastApplyIndex = lastApplyIndex
	kv.lastApplyTerm = lastApplyTerm
	kv.dataBase = dataBase
	kv.recentApplyOpId = recentApplyOpId

}

func (kv *KVServer) applyToDatabaseLoop() {
	for {
		select {
		case <-kv.stopCh:
			{
				return
			}
		case msg := <-kv.applyCh:
			{
				if msg.CommandValid == true {
					op := msg.Command.(Op)
					kv.applyOperationToDataBase(msg, op)
				} else {
					//install snapshot
					snapshotData := msg.Command.([]byte)
					needInstall := kv.rf.CondInstallSnapshot(msg.LogIndex, msg.LogTerm)
					if needInstall == false {
						break
					}
					kv.recoverKVServerSnapshot(snapshotData)
				}
				kv.checkIfNeedSnapshot()
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killed()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	if z == 1 {
		close(kv.stopCh)
	}
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.recentApplyOpId = make(map[int64]int64)
	kv.applyNotify = make(map[string]chan ApplyMsg)
	kv.dataBase = make(map[string]string)

	kv.recoverKVServerSnapshot(persister.ReadSnapshot())

	kv.stopCh = make(chan struct{})

	go kv.applyToDatabaseLoop()

	return kv
}
