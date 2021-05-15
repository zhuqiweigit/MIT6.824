package shardkv

import (
	"../shardmaster"
	"fmt"
	"log"
	"time"
)

func (kv *ShardKV) resetPullConfigTimer() {
	if !kv.pullConfigTimer.Stop() {
		select {
		case <-kv.pullConfigTimer.C:
		default:
		}
	}
	kv.pullConfigTimer.Reset(PullConfigTimeout)
}

func (kv *ShardKV) resetFetchShardsTimer() {
	if !kv.fetchShardsTimer.Stop() {
		select {
		case <-kv.fetchShardsTimer.C:
		default:
		}
	}
	kv.fetchShardsTimer.Reset(FetchShardsTimeout)
}

func (kv *ShardKV) resetSendDelShardsSignalTimer() {
	if !kv.sendDelShardsSignalTimer.Stop() {
		select {
		case <-kv.sendDelShardsSignalTimer.C:
		default:
		}
	}
	kv.sendDelShardsSignalTimer.Reset(SendDelShardsSignalTimeout)
}

func (kv *ShardKV) checkNeedShardsNoAction() bool {
	for _, val := range kv.needShards {
		if val != NoAction {
			return false
		}
	}
	return true
}

//由leader负责pull config
func (kv *ShardKV) pullConfigAndStart() {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		kv.mu.Lock()
		kv.resetPullConfigTimer()
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	if kv.checkNeedShardsNoAction() == false {
		kv.resetPullConfigTimer()
		kv.mu.Unlock()
		return
	}

	desiredConfigNumber := kv.config.Num + 1
	kv.mu.Unlock()

	newconfig := kv.shardmasterClient.Query(desiredConfigNumber)

	if newconfig.Num != desiredConfigNumber {
		kv.mu.Lock()
		kv.resetPullConfigTimer()
		kv.mu.Unlock()
		return
	}

	kv.rf.Start(newconfig)

	kv.mu.Lock()
	kv.resetPullConfigTimer()
	kv.mu.Unlock()
}

//由leader负责抓取新增加的shards
func (kv *ShardKV) fetchShards() {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		kv.mu.Lock()
		kv.resetFetchShardsTimer()
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()

	allNeedFetchShards := make([]int, 0)
	for shid, status := range kv.needShards {
		if status == NeedFetch {
			allNeedFetchShards = append(allNeedFetchShards, shid)
		}
	}
	oldconfigCopy := shardmaster.DupConfig(kv.oldconfig)
	configCopy := shardmaster.DupConfig(kv.config)
	kv.mu.Unlock()

	if len(allNeedFetchShards) == 0 {
		kv.mu.Lock()
		kv.resetFetchShardsTimer()
		kv.mu.Unlock()
		return
	}

	chs := make(chan ShardData, len(allNeedFetchShards))
	for _, needShardsId := range allNeedFetchShards {
		//fetch shard by RPC
		go func(shid int) {
			request := FetchShardsRequest{ShardId: shid, ConfigNum: configCopy.Num}

			gid := oldconfigCopy.Shards[shid]
			groups := oldconfigCopy.Groups[gid]

			for _, serverName := range groups {
				response := FetchShardsReponse{}
				server := kv.make_end(serverName)
				ok := server.Call("ShardKV.GetShard", &request, &response)

				if ok && response.Err == OK {
					chs <- response.ShardData
					return
				} else if ok && response.Err == ErrWrongLeader {
					continue
				} else if ok && (response.Err == ErrOldOperation || response.Err == ErrNoFetchedShard) {
					shdata := ShardData{ConfigNum: -1}
					chs <- shdata
					return
				}
			}
			shdata := ShardData{ConfigNum: -1}
			chs <- shdata
			return
		}(needShardsId)
	}

	for i := 0; i < len(allNeedFetchShards); i++ {
		shardData := <-chs
		if shardData.ConfigNum != configCopy.Num {
			continue
		} else {
			kv.rf.Start(shardData)
		}
	}

	kv.mu.Lock()
	kv.resetFetchShardsTimer()
	kv.mu.Unlock()
}

func (kv *ShardKV) sendDelShardsSignal() {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		kv.mu.Lock()
		kv.resetSendDelShardsSignalTimer()
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	allNeedSendSignalShardsCopy := make(map[string]shardmaster.Config)
	for key, val := range kv.waitToSendDelSignal {
		allNeedSendSignalShardsCopy[key] = shardmaster.DupConfig(val)
	}
	kv.mu.Unlock()

	if len(allNeedSendSignalShardsCopy) == 0 {
		kv.mu.Lock()
		kv.resetSendDelShardsSignalTimer()
		kv.mu.Unlock()
		return
	}

	chs := make(chan bool, len(allNeedSendSignalShardsCopy))

	for configNumShardIdPair, config := range allNeedSendSignalShardsCopy {
		var configNum int
		var shid int
		n, err := fmt.Sscanf(configNumShardIdPair, "%v:%v", &configNum, &shid)
		if n < 2 || err != nil {
			log.Fatal("waitToSendDelSignal key err")
		}
		go func(configNum int, shid int, config shardmaster.Config) {
			request := DelShardsRequest{ShardId: shid, ConfigNum: configNum, Gid: kv.gid}

			gid := config.Shards[shid]
			groups := config.Groups[gid]

			for _, serverName := range groups {
				response := DelShardsResponse{}
				server := kv.make_end(serverName)
				ok := server.Call("ShardKV.DelShard", &request, &response)
				if ok && response.Err == OK {
					cfgNumShardIdPair := fmt.Sprintf("%v:%v", configNum, shid)
					kv.rf.Start(cfgNumShardIdPair)
					chs <- true
					return
				} else if ok && response.Err == ErrWrongLeader {
					continue
				} else if ok && response.Err == ErrOldOperation {
					chs <- false
					return
				} else {
					continue
				}
			}
			chs <- false

		}(configNum, shid, config)
	}
	for i := 0; i < len(allNeedSendSignalShardsCopy); i++ {
		<-chs
	}

	//fixbug: 忘记重置sendDel的timer
	kv.mu.Lock()
	kv.resetSendDelShardsSignalTimer()
	kv.mu.Unlock()
}

func (kv *ShardKV) GetShard(request *FetchShardsRequest, response *FetchShardsReponse) {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	configNumShardIdPair := fmt.Sprintf("%v:%v", request.ConfigNum, request.ShardId)
	if _, ok := kv.discardShards[configNumShardIdPair]; ok == false {
		response.Err = ErrNoFetchedShard
		return
	} else {
		response.Err = OK
		response.ShardData = DupShard(kv.discardShards[configNumShardIdPair])
	}

}

func (kv *ShardKV) DelShard(request *DelShardsRequest, response *DelShardsResponse) {
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		response.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	configNumShardIdPair := fmt.Sprintf("%v:%v", request.ConfigNum, request.ShardId)
	if _, ok := kv.discardShards[configNumShardIdPair]; ok == false {
		response.Err = OK
		kv.mu.Unlock()
		return
	}

	ch := make(chan DelShardsRequest, 1)
	gidShardidPair := fmt.Sprintf("%v:%v", request.Gid, request.ShardId)
	kv.delNotify[gidShardidPair] = ch

	requestCopy := *request
	kv.mu.Unlock()

	//fixbug: 如果start下发的是指针类型request
	//则导致交给本机raft层的requestCopy为struct指针类型，而交给group内其它节点raft层的被自动转换为struct类型；导致在Command强转的时候出现错误
	kv.rf.Start(requestCopy)

	timer := time.NewTimer(time.Millisecond * 5000)
	defer timer.Stop()

	select {
	case <-timer.C:
		{
			response.Err = ErrRaftTimeout
			kv.mu.Lock()
			delete(kv.delNotify, gidShardidPair)
			kv.mu.Unlock()
			return
		}
	case msg := <-ch:
		{
			response.Err = msg.Err
			return
		}
	}

}
