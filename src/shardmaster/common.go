package shardmaster

import "sort"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	clientId  int64
	commandId int64
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	clientId  int64
	commandId int64
	GIDs      []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	clientId  int64
	commandId int64
	Shard     int
	GID       int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

// 复制组
func CopyGroups(config *Config, groups map[int][]string) {
	config.Groups = make(map[int][]string)
	for gid, shards := range groups {
		config.Groups[gid] = shards
	}
}

// 排序
func SortGroup(groups map[int][]string) []int {
	var gids []int
	for gid, _ := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}

// 重新分配组和分片
func DistributionGroups(config *Config) {
	gids := SortGroup(config.Groups)
	for index := 0; index < NShards; {
		for i := 0; i < len(gids); i++ {
			gid := gids[i]
			config.Shards[index] = gid
			index++
			if index >= NShards {
				break
			}
		}
	}
}

// 合并组和分片
func MergeGroups(config *Config, groups map[int][]string) {
	for gid, value := range groups {
		servers, ok := config.Groups[gid]
		if ok {
			servers = append(servers, value...)
		} else {
			for cnt := 0; ; cnt++ {
				//遍历获取分片数量最大组，写入新组。
				maxGroup, maxGroups := GetMaxCountShards(config)
				if cnt >= len(maxGroups)-1 && maxGroup != 0 {
					//分配均匀，完成分配
					break
				}
				//将数量最多组的第一个shard重新分配给新的gid
				config.Shards[maxGroups[0]] = gid
			}
			servers = value
		}
		config.Groups[gid] = servers
	}
}

// 删除组及分片
func DeleteGroups(config *Config, groups []int) {
	for i := 0; i < len(groups); i++ {
		gid := groups[i]
		_, ok := config.Groups[gid]
		if ok {
			//获取该组分片
			shards := config.GetCountGroup(gid)
			//遍历依次加入最小组
			for j := 0; j < len(shards); j++ {
				minGroup := GetMinCountShards(config, gid)
				config.Shards[shards[j]] = minGroup
			}
			//删除组
			delete(config.Groups, gid)
		}
	}
}

// return map from gid to []shards
func GetCountShards(config *Config) map[int][]int {
	rst := make(map[int][]int)
	for gid, _ := range config.Groups {
		rst[gid] = make([]int, 0)
	}
	for i := 0; i < len(config.Shards); i++ {
		gid := config.Shards[i]
		shards, ok := rst[gid]
		if ok {
			shards = append(shards, i)
		} else {
			shards = make([]int, 1)
			shards[0] = i
		}
		rst[gid] = shards
	}
	return rst
}

// return sorted gids
func SortCountShards(gidToShards map[int][]int) []int {
	var gids []int
	for gid, _ := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}

// 获取分片数量最多组
func GetMaxCountShards(config *Config) (group int, rst []int) {
	gidToShards := GetCountShards(config)
	gids := SortCountShards(gidToShards)
	max := 0
	for i := 0; i < len(gids); i++ {
		gid := gids[i]
		shards := gidToShards[gid]
		if len(shards) > max {
			group = gid
			rst = shards
			max = len(shards)
		}
	}
	return
}

// 获得某组下单分片
func (config *Config) GetCountGroup(group int) (rst []int) {
	for i := 0; i < len(config.Shards); i++ {
		if (config.Shards)[i] == group {
			rst = append(rst, i)
		}
	}
	return
}

// 获取分片数量最少组
func GetMinCountShards(config *Config, without int) int {
	rst := 0
	maps := GetCountShards(config)
	gids := SortCountShards(maps)
	min := NShards + 1

	for i := 0; i < len(gids); i++ {
		gid := gids[i]
		value := maps[gid]
		if gid == without {
			continue
		}
		if len(value) < min {
			rst = gid
			min = len(value)
		}
	}
	return rst
}
