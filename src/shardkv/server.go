package shardkv

// import "../shardmaster"
import (
	"bytes"
	"log"
	"sync"
	"time"

	"../labrpc"
	"../raft"
	"../shardmaster"
	"6.824/src/labgob"
)

var shardKvOnce sync.Once

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Ch  chan (interface{})
	Req interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs           [shardmaster.NShards]map[string]string
	msgIDs        map[int64]int64 //clientID -> msgID
	killChan      chan (bool)
	killed        bool
	persister     *raft.Persister
	logApplyIndex int

	config         shardmaster.Config
	nextConfig     shardmaster.Config //下一个config
	notReadyShards map[int][]int      //数据转移过程中的 gid -> shard

	deleteShardsNum int
	mck             *shardmaster.Clerk
	timer           *time.Timer
}

func (kv *ShardKV) isRepeated(client int64, msgId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := false
	index, ok := kv.msgIDs[client]
	if ok {
		rst = index >= msgId
	}
	kv.msgIDs[client] = msgId
	return rst
}

// 判定重复请求
func (kv *ShardKV) isTrueGroup(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num == 0 {
		return false
	}
	group := kv.config.Shards[shard]
	if group == kv.gid {
		return true
	} else if kv.nextConfig.Shards[shard] == kv.gid { //正在转移数据过程中
		_, ok := kv.notReadyShards[group]
		return !ok
	} else {
		return false
	}
}

// 判定cofig更新完成
func (kv *ShardKV) cofigCompleted(Num int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Num >= Num
}

func (kv *ShardKV) opt(req interface{}) (bool, interface{}) {
	op := Op{
		Req: req,                         //请求数据
		Ch:  make(chan (interface{}), 1), //日志提交chan
	}
	_, _, isLeader := kv.rf.Start(op) //写入Raft
	if !isLeader {                    //判定是否是leader
		return false, nil
	}
	select {
	case resp := <-op.Ch:
		return true, resp
	case <-time.After(time.Millisecond * 1000): //超时
	}
	return false, nil
}

func (kv *ShardKV) updateSnapshot(index int, data []byte) {
	if data == nil || len(data) < 1 || kv.maxraftstate == -1 {
		return
	}
	kv.logApplyIndex = index
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	for i := 0; i < len(kv.kvs); i++ {
		kv.kvs[i] = make(map[string]string)
	}
	kv.notReadyShards = make(map[int][]int)
	if decoder.Decode(&kv.msgIDs) != nil ||
		decoder.Decode(&kv.kvs) != nil ||
		decoder.Decode(&kv.config) != nil ||
		decoder.Decode(&kv.nextConfig) != nil ||
		decoder.Decode(&kv.notReadyShards) != nil {
	}
}

// 判定是否写入快照
func (kv *ShardKV) ifSaveSnapshot(save bool) {
	if kv.maxraftstate != -1 && (kv.persister.RaftStateSize() >= kv.maxraftstate || save) {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)
		encoder.Encode(kv.msgIDs)
		encoder.Encode(kv.kvs)
		encoder.Encode(kv.config)
		encoder.Encode(kv.nextConfig)
		encoder.Encode(kv.notReadyShards)
		data := writer.Bytes()
		kv.rf.SaveSnapshot(kv.logApplyIndex, data)
		return
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok, value := kv.opt(*args)
	if ok {
		*reply = value.(GetReply)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ok, value := kv.opt(*args)
	if ok {
		*reply = value.(PutAppendReply)
	}
}

// 请求分片数据
func (kv *ShardKV) GetShard(req *ReqShared, reply *RespShared) {
	ok, value := kv.opt(*req)
	reply.Successed = false
	if ok {
		*reply = value.(RespShared)
	}
}

// 删除已发送分片
func (kv *ShardKV) DeleteShards(req *ReqDeleteShared, resp *RespDeleteShared) {
	if req.ConfigNum > kv.deleteShardsNum {
		if kv.config.Num == req.ConfigNum || kv.config.Num == req.ConfigNum+1 {
			kv.deleteShardsNum = req.ConfigNum
			kv.opt(*req)
		}
	}
}

func (kv *ShardKV) get(req *GetArgs) (resp GetReply) {
	if !kv.isTrueGroup(req.Shard) {
		resp.Err = ErrWrongGroup
		return
	}
	value, ok := kv.kvs[req.Shard][req.Key]
	resp.Err = OK
	if !ok {
		value = ""
		resp.Err = ErrNoKey
	}
	resp.Value = value
	return
}

func (kv *ShardKV) putAppend(req *PutAppendArgs) Err {
	if !kv.isTrueGroup(req.Shard) {
		return ErrWrongGroup
	}
	if !kv.isRepeated(req.Me, req.MsgId) { //去重复
		if req.Op == "Put" {
			kv.kvs[req.Shard][req.Key] = req.Value
		} else if req.Op == "Append" {
			value, ok := kv.kvs[req.Shard][req.Key]
			if !ok {
				value = ""
			}
			value += req.Value
			kv.kvs[req.Shard][req.Key] = value
		}
	}
	return OK
}

// 获取新增shards
func (kv *ShardKV) getNewShards() (map[int][]int, bool) {
	shards := make(map[int][]int) //gid -> shard
	if kv.nextConfig.Num > kv.config.Num {
		if kv.nextConfig.Num > 1 {
			oldShards := GetGroupShards(&(kv.config.Shards), kv.gid)     //旧该组分片
			newShards := GetGroupShards(&(kv.nextConfig.Shards), kv.gid) //新该组分片
			for key, _ := range newShards {                              //获取新增组, key是shard号
				_, ok := oldShards[key]
				if !ok {
					gid := kv.config.Shards[key]
					value, ok := shards[gid]
					if !ok {
						value = make([]int, 0)
					}
					value = append(value, key)
					shards[gid] = value
				}
			}
		}
		return shards, true
	}
	return shards, false
}

// config更新
func (kv *ShardKV) onConfig(config *shardmaster.Config) {
	if config.Num > kv.nextConfig.Num {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.nextConfig = *config
		kv.notReadyShards, _ = kv.getNewShards()
	}
}

// 收到请求，将数据发送给申请组
func (kv *ShardKV) getShard(req *ReqShared) (resp RespShared) {
	if req.ConfigNum > kv.config.Num { //自己未获取最新数据
		resp.Successed = false
		kv.timer.Reset(0) //获取最新数据
		return
	}
	resp.Successed = true
	resp.Group = kv.gid
	//复制已处理消息
	resp.MsgIDs = make(map[int64]int64)
	for key, value := range kv.msgIDs {
		resp.MsgIDs[key] = value
	}
	//复制分片数据
	resp.Data = make(map[int]map[string]string)
	for i := 0; i < len(req.Shards); i++ {
		shard := req.Shards[i]
		data := kv.kvs[shard]
		shardDatas := make(map[string]string)
		for key, value := range data {
			shardDatas[key] = value
		}
		resp.Data[shard] = shardDatas
	}
	return
}

// 将别的组的数据配置到自己组
func (kv *ShardKV) setShard(resp *RespShared) {
	kv.mu.Lock()
	_, ok := kv.notReadyShards[resp.Group]
	if !ok {
		return
	}
	delete(kv.notReadyShards, resp.Group)
	//更新数据
	for shard, kvs := range resp.Data {
		for key, data := range kvs {
			kv.kvs[shard][key] = data
		}
	}
	//更新msgid
	for key, value := range resp.MsgIDs {
		id, ok := kv.msgIDs[key]
		if !ok || id < value {
			kv.msgIDs[key] = value
		}
	}
	preConfig := kv.config
	kv.mu.Unlock()
	go kv.deleteGroupShards(&preConfig, resp)
}

// 设置分片数据
func (kv *ShardKV) setShards(resp *RespShareds) {
	if kv.cofigCompleted(resp.ConfigNum) { //数据已更新
		return
	}
	kv.mu.Lock()
	kv.config = kv.nextConfig
	kv.mu.Unlock()
}

// 删除已发送分片
func (kv *ShardKV) deleteShards(req *ReqDeleteShared) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num == req.ConfigNum || kv.config.Num == req.ConfigNum+1 {
		for i := 0; i < len(req.Shards); i++ {
			shard := req.Shards[i]
			kv.kvs[shard] = make(map[string]string)
		}
	}
}

// 请求删除已获得分片
func (kv *ShardKV) deleteGroupShards(config *shardmaster.Config, respShard *RespShared) {
	req := ReqDeleteShared{
		ConfigNum: config.Num + 1,
	}
	for shard, _ := range respShard.Data {
		req.Shards = append(req.Shards, shard)
	}
	servers, ok := config.Groups[respShard.Group] //获取目标组服务
	if !ok {
		return
	}
	resp := RespDeleteShared{}
	for i := 0; i < len(servers); i++ {
		server := kv.make_end(servers[i])
		server.Call("ShardKV.DeleteShards", &req, &resp)
	}
}

// raft更新config
func (kv *ShardKV) startConfig(config *shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num > kv.nextConfig.Num && kv.nextConfig.Num == kv.config.Num {
		op := Op{
			Req: *config,                  //请求数据
			Ch:  make(chan (interface{})), //日志提交chan
		}
		kv.rf.Start(op) //写入Raft
	}
}

// 获取新配置
func (kv *ShardKV) updateConfig() {
	if _, isLeader := kv.rf.GetState(); isLeader {
		if kv.nextConfig.Num == kv.config.Num {
			config := kv.mck.Query(kv.nextConfig.Num + 1)
			kv.startConfig(&config)
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// apply 状态机
func (kv *ShardKV) Apply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid { //非状态机apply消息
		if command, ok := applyMsg.Command.(raft.LogSnapshot); ok { //更新快照消息
			kv.updateSnapshot(command.Index, command.Datas)
		} else {
			kv.ifSaveSnapshot(false)
		}
		return
	}
	//更新日志索引，用于创建最新快照
	kv.logApplyIndex = applyMsg.CommandIndex
	opt := applyMsg.Command.(Op)
	var resp interface{}
	if command, ok := opt.Req.(PutAppendArgs); ok { //Put && append操作
		resp = kv.putAppend(&command)
	} else if command, ok := opt.Req.(GetArgs); ok { //Get操作
		resp = kv.get(&command)
	} else if command, ok := opt.Req.(shardmaster.Config); ok { //更新config
		kv.onConfig(&command)
	} else if command, ok := opt.Req.(ReqShared); ok { //其他组获取该组分片数据
		resp = kv.getShard(&command)
	} else if command, ok := opt.Req.(RespShared); ok { //获取其他组分片
		kv.setShard(&command)
	} else if command, ok := opt.Req.(RespShareds); ok { //获取其他组分片
		kv.setShards(&command)
	} else if command, ok := opt.Req.(ReqDeleteShared); ok {
		kv.deleteShards(&command)
		kv.ifSaveSnapshot(true) //强制刷新快照数据
	}
	select {
	case opt.Ch <- resp:
	default:
	}
}

func (kv *ShardKV) mainLoop() {
	duration := time.Duration(time.Millisecond * 40)
	for !kv.killed {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh:
			if cap(kv.applyCh)-len(kv.applyCh) < 5 {
				log.Println(kv.gid, kv.me, "warn : maybe dead lock...")
			}
			kv.Apply(msg)
		case <-kv.timer.C:
			kv.updateConfig()
			kv.timer.Reset(duration)
		}
	}
}

func (kv *ShardKV) isLeader() bool {
	_, rst := kv.rf.GetState()
	return rst
}

// 判定组分片是否完成
func (kv *ShardKV) isReadyShards(group int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if group == -1 {
		return len(kv.notReadyShards) == 0
	}
	_, ok := kv.notReadyShards[group]
	return !ok
}

// 获取是否更新config及新增组/分片
func (kv *ShardKV) isUpdateConfig() (bool, map[int][]int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := make(map[int][]int)
	for key, value := range kv.notReadyShards {
		rst[key] = value
	}
	return kv.nextConfig.Num > kv.config.Num, rst
}

// 遍历所有分片，请求数据
func (kv *ShardKV) getShardFromOther(group int, shards []int, ch chan bool, num int) {
	defer func() { ch <- true }()
	complet := false
	var resp RespShared
	for !complet && !(kv.cofigCompleted(num)) && !kv.killed {
		servers, ok := kv.config.Groups[group] //获取目标组服务
		if !ok {
			time.Sleep(time.Millisecond * 500)
			continue
		}
		req := ReqShared{
			ConfigNum: kv.nextConfig.Num,
			Shards:    shards,
		}
		for i := 0; i < len(servers); i++ {
			server := kv.make_end(servers[i])
			ok := server.Call("ShardKV.GetShard", &req, &resp)
			if ok && resp.Successed {
				complet = true
				break
			}
			if kv.cofigCompleted(num) || kv.killed {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
	}
	//存储该分片数据
	for !kv.killed && !kv.cofigCompleted(num) && !kv.isReadyShards(group) && kv.isLeader() {
		successed, _ := kv.opt(resp)
		if successed {
			break
		}
	}
}

// raft更新shard
func (kv *ShardKV) startShard(shards *RespShareds) {
	kv.mu.Lock()
	if kv.config.Num < kv.nextConfig.Num {
		kv.mu.Unlock()
		op := Op{
			Req: *shards,                     //请求数据
			Ch:  make(chan (interface{}), 1), //日志提交chan
		}
		index, term, isLeader := kv.rf.Start(op) //写入Raft
		if isLeader {
			select {
			case <-op.Ch:
				DPrintf(kv.gid, kv.me, "on start shard ", kv.nextConfig.Num, "term", term, "index", index)
			case <-time.After(time.Millisecond * 1000): //超时
				DPrintf(kv.gid, kv.me, "start shard timeout", kv.nextConfig.Num, "term", term, "index", index)
			}
		}
	} else {
		kv.mu.Unlock()
	}
}

// 请求其他组数据
func (kv *ShardKV) shardLoop() {
	for !kv.killed {
		if !kv.isLeader() {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		isUpdated, shards := kv.isUpdateConfig()
		if !isUpdated {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		config := kv.nextConfig
		Num := config.Num
		waitCh := make(chan bool, len(shards))
		for key, value := range shards {
			go kv.getShardFromOther(key, value, waitCh, Num)
		}
		for !kv.killed && kv.isLeader() {
			if kv.cofigCompleted(Num) {
				break
			}
			if kv.isReadyShards(-1) {
				break
			}
			select {
			case <-waitCh:
			case <-time.After(time.Millisecond * 500): //超时
			}
		}
		if !kv.isReadyShards(-1) {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		//获取的状态写入RAFT，直到成功
		for !(kv.cofigCompleted(Num)) && !kv.killed && kv.isLeader() {
			respShards := RespShareds{
				ConfigNum: Num,
			}
			kv.startShard(&respShards)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

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
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvs[i] = make(map[string]string)
	}
	kv.msgIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool), 1)
	kv.persister = persister
	kv.logApplyIndex = 0
	shardKvOnce.Do(func() {
		labgob.Register(Op{})
		labgob.Register(PutAppendArgs{})
		labgob.Register(GetArgs{})
		labgob.Register(ReqShared{})
		labgob.Register(RespShared{})
		labgob.Register(RespShareds{})
		labgob.Register(ReqDeleteShared{})
		labgob.Register(RespDeleteShared{})
		labgob.Register(shardmaster.Config{})
	})
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.rf.EnableDebugLog = false
	kv.config.Num = 0
	kv.nextConfig.Num = 0
	kv.notReadyShards = make(map[int][]int)
	kv.deleteShardsNum = 0
	kv.killed = false
	kv.timer = time.NewTimer(time.Duration(time.Millisecond * 100))
	go kv.mainLoop()
	go kv.shardLoop()

	return kv
}
