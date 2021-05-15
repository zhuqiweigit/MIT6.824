package shardkv

import "../raft"
import "../shardmaster"
import "fmt"

func (kv *ShardKV) executeOperation(op Op, raftApplyMsg raft.ApplyMsg) {
	kv.mu.Lock()

	msg := NotifyMsg{}
	shardId := key2shard(op.Key)
	clientOperationIdPair := fmt.Sprintf("%v:%v", op.ClientId, op.OperationId)

	if _, ok := kv.dataBase[shardId]; ok == false {
		msg.Err = ErrWrongGroup
		if _, ok := kv.applyNotify[clientOperationIdPair]; ok == false {
			kv.mu.Unlock()
			return
		}
		ch := kv.applyNotify[clientOperationIdPair]
		delete(kv.applyNotify, clientOperationIdPair)
		kv.mu.Unlock()
		ch <- msg
		return
	}

	if _, ok := kv.dataBase[shardId].RecentApplyOpId[op.ClientId]; ok == false {
		kv.dataBase[shardId].RecentApplyOpId[op.ClientId] = -1
	}

	if op.OperationId < kv.dataBase[shardId].RecentApplyOpId[op.ClientId] {
		msg.Err = ErrOldOperation
	} else if op.OperationId == kv.dataBase[shardId].RecentApplyOpId[op.ClientId] {
		kv.lastApplyIndex = raftApplyMsg.LogIndex
		kv.lastApplyTerm = raftApplyMsg.LogTerm

		if op.Method == "Get" {
			val, ok := kv.dataBase[shardId].Data[op.Key]
			if ok == false {
				msg.Err = ErrNoKey
				msg.Value = ""
			} else {
				msg.Err = OK
				msg.Value = val
			}
		} else {
			msg.Err = OK
		}
	} else if op.OperationId > kv.dataBase[shardId].RecentApplyOpId[op.ClientId] {
		kv.lastApplyIndex = raftApplyMsg.LogIndex
		kv.lastApplyTerm = raftApplyMsg.LogTerm

		if op.Method == "Get" {
			val, ok := kv.dataBase[shardId].Data[op.Key]
			if ok == false {
				msg.Err = ErrNoKey
				msg.Value = ""
			} else {
				msg.Err = OK
				msg.Value = val
			}
		} else if op.Method == "Put" {
			kv.dataBase[shardId].Data[op.Key] = op.Value
			msg.Err = OK
		} else if op.Method == "Append" {
			val, ok := kv.dataBase[shardId].Data[op.Key]
			if ok == false {
				kv.dataBase[shardId].Data[op.Key] = op.Value
			} else {
				val = val + op.Value
				kv.dataBase[shardId].Data[op.Key] = val
			}
			msg.Err = OK
		}
		kv.dataBase[shardId].RecentApplyOpId[op.ClientId] = op.OperationId
	}

	if _, ok := kv.applyNotify[clientOperationIdPair]; ok == false {
		kv.mu.Unlock()
		return
	}
	ch := kv.applyNotify[clientOperationIdPair]
	delete(kv.applyNotify, clientOperationIdPair)

	kv.mu.Unlock()
	ch <- msg
}

func (kv *ShardKV) executeConfig(newconfig shardmaster.Config, raftApplyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastApplyIndex = raftApplyMsg.LogIndex
	kv.lastApplyTerm = raftApplyMsg.LogTerm

	//当前的config待删or传送的shard没有传送、删除完以前，不能执行新的config
	if kv.checkNeedShardsNoAction() == false {
		return
	}

	//不是期待的config，直接返回
	if newconfig.Num != kv.config.Num+1 {
		return
	}

	newConfigShardId := make([]int, 0)
	for shid, gid := range newconfig.Shards {
		if gid == kv.gid {
			newConfigShardId = append(newConfigShardId, shid)
		}
	}

	//首次接收config
	if kv.config.Num == 0 {
		for _, shid := range newConfigShardId {
			kv.dataBase[shid] = ShardData{ShardId: shid, ConfigNum: newconfig.Num, Data: make(map[string]string), RecentApplyOpId: make(map[int64]int64)}
		}
		kv.config = shardmaster.DupConfig(newconfig)
		return
	}

	deletedShardId := make([]int, 0)
	neededShardId := make([]int, 0)
	reservedShardId := make([]int, 0)
	for _, shid := range newConfigShardId {
		if _, ok := kv.dataBase[shid]; ok == true {
			reservedShardId = append(reservedShardId, shid)
		} else {
			neededShardId = append(neededShardId, shid)
		}
	}
	for shid, _ := range kv.dataBase {
		finded := false
		for _, newshid := range newConfigShardId {
			if newshid == shid {
				finded = true
				break
			}
		}
		if finded == false {
			deletedShardId = append(deletedShardId, shid)
		}
	}

	for _, shid := range neededShardId {
		kv.needShards[shid] = NeedFetch
	}

	for _, shid := range deletedShardId {
		dupShard := DupShard(kv.dataBase[shid])
		dupShard.ConfigNum = newconfig.Num
		configNumShardIdPair := fmt.Sprintf("%v:%v", newconfig.Num, shid)
		kv.discardShards[configNumShardIdPair] = dupShard
		delete(kv.dataBase, shid)
	}

	kv.oldconfig = shardmaster.DupConfig(kv.config)
	kv.config = shardmaster.DupConfig(newconfig)

}

func (kv *ShardKV) executeAddedShard(shardData ShardData, raftApplyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastApplyIndex = raftApplyMsg.LogIndex
	kv.lastApplyTerm = raftApplyMsg.LogTerm

	if shardData.ConfigNum != kv.config.Num {
		return
	}

	if _, ok := kv.dataBase[shardData.ShardId]; ok == false {
		kv.dataBase[shardData.ShardId] = DupShard(shardData)
	}

	if kv.needShards[shardData.ShardId] == NeedFetch {
		configNumShardIdPair := fmt.Sprintf("%v:%v", kv.config.Num, shardData.ShardId)
		kv.waitToSendDelSignal[configNumShardIdPair] = shardmaster.DupConfig(kv.oldconfig)
		kv.needShards[shardData.ShardId] = NoAction
	}

}

func (kv *ShardKV) executeDelShard(delShardRequest DelShardsRequest, raftApplyMsg raft.ApplyMsg) {
	kv.mu.Lock()

	kv.lastApplyIndex = raftApplyMsg.LogIndex
	kv.lastApplyTerm = raftApplyMsg.LogTerm

	gidShardidPair := fmt.Sprintf("%v:%v", delShardRequest.Gid, delShardRequest.ShardId)

	configNumShardIdPair := fmt.Sprintf("%v:%v", delShardRequest.ConfigNum, delShardRequest.ShardId)

	delete(kv.discardShards, configNumShardIdPair)

	msg := DelShardsRequest{Err: OK}
	if _, ok := kv.delNotify[gidShardidPair]; ok == false {
		kv.mu.Unlock()
		return
	}
	ch := kv.delNotify[gidShardidPair]
	delete(kv.delNotify, gidShardidPair)

	kv.mu.Unlock()
	ch <- msg
}

func (kv *ShardKV) executeDelWaitToSendDelSignalList(configNumShardIdPair string, raftApplyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastApplyIndex = raftApplyMsg.LogIndex
	kv.lastApplyTerm = raftApplyMsg.LogTerm

	delete(kv.waitToSendDelSignal, configNumShardIdPair)
}
