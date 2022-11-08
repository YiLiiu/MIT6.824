package shardmaster

import (
	"log"
	"sync"
	"time"

	"../labrpc"
	"../raft"
	"6.824/src/labgob"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	msgIDs   map[int64]int64
	killChan chan (bool)

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Ch  chan (interface{})
	Req interface{}
}

// 获取历史配置
func (sm *ShardMaster) getConfig(index int, config *Config) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if index >= 0 && index < len(sm.configs) {
		*config = sm.configs[index]
		return true
	}
	*config = sm.configs[len(sm.configs)-1]
	return false
}

// 获取最新配置
func (sm *ShardMaster) getCurrentConfig() Config {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config := sm.configs[len(sm.configs)-1]
	CopyGroups(&config, config.Groups)
	return config
}

// 更新配置
func (sm *ShardMaster) appendConfig(config *Config) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, *config)
}

// raft操作
func (sm *ShardMaster) opt(client int64, msgId int64, req interface{}) (bool, interface{}) {
	if msgId > 0 && sm.isRepeated(client, msgId, false) {
		return true, nil
	}
	op := Op{
		Req: req,                      //请求数据
		Ch:  make(chan (interface{})), //日志提交chan
	}
	_, _, isLeader := sm.rf.Start(op) //写入Raft
	if !isLeader {
		return false, nil //判定是否是leader
	}
	select {
	case resp := <-op.Ch:
		return true, resp
	case <-time.After(time.Millisecond * 800): //超时
	}
	return false, nil
}

// 判断重复命令
func (sm *ShardMaster) isRepeated(client int64, msgId int64, update bool) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	rst := false
	index, ok := sm.msgIDs[client]
	if ok {
		rst = index >= msgId
	}
	if update && !rst {
		sm.msgIDs[client] = msgId
	}
	return rst
}

//client request

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	ok, _ := sm.opt(args.clientId, args.commandId, *args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	ok, _ := sm.opt(args.clientId, args.commandId, *args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	ok, _ := sm.opt(args.clientId, args.commandId, *args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sm.getConfig(args.Num, &reply.Config) {
		reply.WrongLeader = false
		return
	}
	ok, resp := sm.opt(-1, -1, *args) //查询操作
	if ok {
		reply.Config = resp.(Config)
	}
	reply.WrongLeader = !ok
}

//sever action

func (sm *ShardMaster) join(args *JoinArgs) bool {
	if sm.isRepeated(args.clientId, args.commandId, true) { //去重复
		return true
	}
	config := sm.getCurrentConfig() //最新配置
	if config.Num == 0 {            //如果第一次配置，则重分配
		config.Groups = args.Servers
		DistributionGroups(&config) //重分配分片与组
	} else {
		MergeGroups(&config, args.Servers) //合并组
	}
	sm.appendConfig(&config)
	return true
}

func (sm *ShardMaster) leave(args *LeaveArgs) bool {
	if sm.isRepeated(args.clientId, args.commandId, true) {
		return true
	}
	config := sm.getCurrentConfig()
	DeleteGroups(&config, args.GIDs) //删除组
	sm.appendConfig(&config)
	return true
}

func (sm *ShardMaster) move(args *MoveArgs) bool {
	if sm.isRepeated(args.clientId, args.commandId, true) {
		return true
	}
	config := sm.getCurrentConfig()
	config.Shards[args.Shard] = args.GID
	sm.appendConfig(&config)
	return true
}

func (sm *ShardMaster) query(args *QueryArgs) Config {
	reply := Config{}
	sm.getConfig(args.Num, &reply)
	return reply
}

func (sm *ShardMaster) Apply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid { //非状态机apply消息
		return
	}
	op := applyMsg.Command.(Op)
	var resp interface{}
	if command, ok := op.Req.(JoinArgs); ok {
		resp = sm.join(&command)
	} else if command, ok := op.Req.(LeaveArgs); ok {
		resp = sm.leave(&command)
	} else if command, ok := op.Req.(MoveArgs); ok {
		resp = sm.move(&command)
	} else {
		command := op.Req.(QueryArgs)
		resp = sm.query(&command)
	}
	select {
	case op.Ch <- resp:
	default:
	}
}

func (sm *ShardMaster) mainLoop() {
	for {
		select {
		case <-sm.killChan:
			return
		case msg := <-sm.applyCh:
			if cap(sm.applyCh)-len(sm.applyCh) < 5 {
				log.Println("warn : maybe dead lock...")
			}
			sm.Apply(msg)
		}
	}
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.msgIDs = make(map[int64]int64)
	sm.killChan = make(chan (bool))
	go sm.mainLoop()

	return sm
}
