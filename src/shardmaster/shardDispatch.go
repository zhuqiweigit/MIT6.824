package shardmaster

func (sm *ShardMaster) joinGroups(groups map[int][]string) {

	newConfig := sm.makeNewConfig()
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; ok == false {
			newConfig.Groups[gid] = make([]string, len(servers))
			copy(newConfig.Groups[gid], servers)
		} else {
			newConfig.Groups[gid] = servers
		}
	}

	var avgNum int
	avgNum = NShards / len(newConfig.Groups)
	if avgNum == 0 {
		avgNum = 1
	}
	group2shards := sm.genGroup2ShardsMap(&newConfig)
	for _, shards := range group2shards {
		if len(shards) > avgNum {
			deleteShards := shards[avgNum:]
			shards = shards[0:avgNum]
			for _, shid := range deleteShards {
				newConfig.Shards[shid] = 0
			}
		}
	}

	sm.rebanlance(&newConfig, group2shards)

	sm.configs = append(sm.configs, newConfig)

}

func (sm *ShardMaster) leaveGroups(gids []int) {

	newConfig := sm.makeNewConfig()
	group2shards := sm.genGroup2ShardsMap(&newConfig)

	for _, gid := range gids {
		if delShards, ok := group2shards[gid]; ok == true {
			for _, shid := range delShards {
				newConfig.Shards[shid] = 0
			}
		}
		delete(group2shards, gid)
		delete(newConfig.Groups, gid)
	}

	if len(newConfig.Groups) == 0 {
		sm.configs = append(sm.configs, newConfig)
		return
	}

	sm.rebanlance(&newConfig, group2shards)

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) moveGroup(gid int, shid int) {

	newConfig := sm.makeNewConfig()
	newConfig.Shards[shid] = gid

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) query(num int) Config {

	lastIdx := len(sm.configs) - 1
	targetIdx := -1
	if num == -1 || num > lastIdx {
		targetIdx = lastIdx
	} else {
		targetIdx = num
	}

	targetConfig := sm.configs[targetIdx]
	queryConfig := Config{Num: targetConfig.Num, Shards: targetConfig.Shards}
	queryConfig.Groups = make(map[int][]string)
	for gid, servers := range targetConfig.Groups {
		queryConfig.Groups[gid] = make([]string, len(servers))
		copy(queryConfig.Groups[gid], servers)
	}
	return queryConfig
}

func (sm *ShardMaster) makeNewConfig() Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards}

	newConfig.Groups = make(map[int][]string)
	for gid, servers := range lastConfig.Groups {
		if gid == 0 {
			continue
		}
		newConfig.Groups[gid] = make([]string, len(servers))
		copy(newConfig.Groups[gid], servers)
	}
	return newConfig
}

func (sm *ShardMaster) genGroup2ShardsMap(newConfig *Config) map[int][]int {

	group2shards := make(map[int][]int)
	for gid, _ := range newConfig.Groups {
		if gid == 0 {
			continue
		}
		group2shards[gid] = make([]int, 0)
	}

	//todo: fixbug
	for shid := 0; shid < NShards; shid++ {
		gid := newConfig.Shards[shid]
		if gid == 0 {
			continue
		} else {
			group2shards[gid] = append(group2shards[gid], shid)
		}
	}
	return group2shards
}

//返回拥有最多shards的gid，如果有多个，返回gid最小的
func (sm *ShardMaster) getMaxGid(group2shards map[int][]int) int {
	maxLen := 0
	minGid := -1
	for gid, shards := range group2shards {
		if maxLen < len(shards) || maxLen == len(shards) && gid < minGid {
			minGid = gid
			maxLen = len(shards)
		}
	}
	return minGid
}

func (sm *ShardMaster) getMinGid(group2shards map[int][]int) int {
	minLen := NShards + 1
	minGid := -1
	for gid, shards := range group2shards {
		if minLen > len(shards) || minLen == len(shards) && gid < minGid {
			minGid = gid
			minLen = len(shards)
		}
	}
	return minGid
}

func (sm *ShardMaster) rebanlance(newConfig *Config, group2shards map[int][]int) {
	for shid := 0; shid < NShards; shid++ {
		if newConfig.Shards[shid] == 0 {
			gid := sm.getMinGid(group2shards)
			newConfig.Shards[shid] = gid
			group2shards[gid] = append(group2shards[gid], shid)
		}
	}
}
