package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var kvOnce sync.Once

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Ch  chan (interface{})
	Req interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	kvs           map[string]string
	commandIDs    map[int64]int64
	killChan      chan (bool)
	persister     *raft.Persister
	logApplyIndex int
}

// raft操作
func (kv *KVServer) opt(client int64, commandId int64, req interface{}) (bool, interface{}) {
	if commandId > 0 && kv.isRepeated(client, commandId, false) { //去重
		return true, nil
	}
	op := Op{
		Req: req,                      //请求数据
		Ch:  make(chan (interface{})), //日志提交chan
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

// 判定重复请求
func (kv *KVServer) isRepeated(client int64, commandId int64, update bool) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := false
	index, ok := kv.commandIDs[client]
	if ok {
		rst = index >= commandId
	}
	if update && !rst {
		kv.commandIDs[client] = commandId
	}
	return rst
}

// 读请求
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok, value := kv.opt(-1, -1, *args)
	reply.wrongLeader = !ok
	if ok {
		reply.Value = value.(string)
	}
}

// 写请求
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ok, _ := kv.opt(args.clientId, args.sequenceNum, *args)
	reply.wrongLeader = ok
}

// 读操作
func (kv *KVServer) get(args *GetArgs) (value string) {
	value, ok := kv.kvs[args.Key]
	if !ok {
		value = ""
	}
	return
}

// 写操作
func (kv *KVServer) putAppend(req *PutAppendArgs) {
	if req.Op == "Put" {
		kv.kvs[req.Key] = req.Value
	} else if req.Op == "Append" {
		value, ok := kv.kvs[req.Key]
		if !ok {
			value = ""
		}
		value += req.Value
		kv.kvs[req.Key] = value
	}
}

// 判定是否写入快照
func (kv *KVServer) ifSaveSnapshot() {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)
		encoder.Encode(kv.commandIDs)
		encoder.Encode(kv.kvs)
		data := writer.Bytes()
		kv.rf.SaveSnapshot(kv.logApplyIndex, data)
		return
	}
}

// 更新快照
func (kv *KVServer) updateSnapshot(index int, data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	kv.logApplyIndex = index
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	if decoder.Decode(&kv.commandIDs) != nil ||
		decoder.Decode(&kv.kvs) != nil {
		DPrintf("Error in unmarshal raft state")
	}
}

// apply
func (kv *KVServer) Apply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid { //非状态机apply消息
		if command, ok := applyMsg.Command.(raft.LogSnapshot); ok { //更新快照消息
			kv.updateSnapshot(command.index, command.datas)
		}
		kv.ifSaveSnapshot()
		return
	}
	//更新日志索引，用于创建最新快照
	kv.logApplyIndex = applyMsg.CommandIndex
	opt := applyMsg.Command.(Op)
	var resp interface{}
	if command, ok := opt.Req.(PutAppendArgs); ok { //Put && append操作
		if !kv.isRepeated(command.clientId, command.sequenceNum, true) { //去重复
			kv.putAppend(&command)
		}
		resp = true
	} else { //Get操作
		command := opt.Req.(GetArgs)
		resp = kv.get(&command)
	}
	select {
	case opt.Ch <- resp:
	default:
	}
	kv.ifSaveSnapshot()
}

func (kv *KVServer) mainLoop() {
	for {
		select {
		case <-kv.killChan:
			return
		case applyMsg := <-kv.applyCh:
			if cap(kv.applyCh)-len(kv.applyCh) < 5 {
				log.Println("warn : maybe dead lock...")
			}
			kv.Apply(applyMsg)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.commandIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool))
	kv.persister = persister
	kv.logApplyIndex = 0
	kvOnce.Do(func() {
		labgob.Register(Op{})
		labgob.Register(PutAppendArgs{})
		labgob.Register(GetArgs{})
	})
	go kv.mainLoop()
	return kv
}
