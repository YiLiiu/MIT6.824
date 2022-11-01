package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"6.824/src/labgob"

	"6.824/src/labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.

// 节点状态
const Follower, Leader, Candidate int = 1, 2, 3

// 心跳周期
const HeartbeatDuration = time.Duration(time.Millisecond * 600)

// 竞选周期
const CandidateDuration = HeartbeatDuration * 2

var raftOnce sync.Once

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry 日志条目
type LogEntry struct {
	log   interface{} //日志记录的命令(用于应用服务的命令)
	index int         //该日志的索引
	term  int         //该日志被接收的时候的Leader任期
}

// 日志快照
type LogSnapshot struct {
	term  int
	index int
	datas []byte
}

// 投票请求
type RequestVoteArgs struct {
	candidateId  int
	term         int
	lastLogIndex int
	lastLogTerm  int
}

// 投票rpc返回
type RequestVoteReply struct {
	voteGranted bool
	term        int
}

// 日志复制请求
type AppendEntries struct {
	leaderId     int
	term         int
	prevLogTerm  int
	prevLogIndex int
	entries      []LogEntry
	leaderCommit int
	snapshot     LogSnapshot //快照
}

// 回复日志更新请求
type RespEntries struct {
	term        int
	successed   bool
	lastApplied int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      bool                // set by Kill()
	// Your data here (2A, 2B, 2C).// Look at the paper's Figure 2 for a description of what// state a Raft server must maintain.
	status int //当前raft状态
	//persistent state on all servers
	currentTerm int //当前任期
	//voteFor     int        //当前任期投给的候选人id(为-1时代表没有投票)
	logEntries []LogEntry //日志条目
	//volatile state on all servers
	commitIndex int //当前log中的最高索引(从0开始,递增)
	lastApplied int //当前被用到状态机中的日志最高索引(从0开始,递增)
	//volatile state on leaders
	nextIndex  []int //leader为每个sever维护的数组，保存leader试图和每个sever相匹配的index
	matchIndex []int

	heartbeatTimers []*time.Timer //心跳定时器
	eletionTimer    *time.Timer   //竞选超时定时器
	randtime        *rand.Rand    //随机数，用于随机竞选周期，避免节点间竞争。

	applyCh chan ApplyMsg //命令应用通道

	lastLogs       AppendEntries //最后更新日志
	EnableDebugLog bool          //打印调试日志开关
	//最近快照的数据
	logSnapshot LogSnapshot //日志快照
	LastGetLock string
}

// 打印调试日志
func (rf *Raft) println(args ...interface{}) {
	if rf.EnableDebugLog {
		log.Println(args...)
	}
}
func (rf *Raft) lock(info string) {
	rf.mu.Lock()
	rf.LastGetLock = info
}

func (rf *Raft) unlock(info string) {
	rf.LastGetLock = ""
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 投票RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		fmt.Printf("sRV: %v\n", args)
		rst := rf.peers[server].Call("Raft.RequestVote", args, reply)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(HeartbeatDuration):
		//rpc调用超时
	}
	return ok
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	rf.lock("Raft.persist")
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.logEntries)
	encoder.Encode(rf.lastLogs)
	encoder.Encode(rf.logSnapshot.index)
	encoder.Encode(rf.logSnapshot.term)
	data := writer.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.unlock("Raft.persist")
	rf.persister.SaveStateAndSnapshot(data, rf.logSnapshot.datas)
	//持久化数据后，apply 通知触发快照
	msg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: 0,
	}
	rf.applyCh <- msg
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.lock("Raft.readPersist")
	if data == nil || len(data) < 1 {
		rf.unlock("Raft.readPersist")
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var commitIndex, currentTerm int
	var logs []LogEntry
	var lastlogs AppendEntries
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&lastlogs) != nil ||
		decoder.Decode(&rf.logSnapshot.index) != nil ||
		decoder.Decode(&rf.logSnapshot.term) != nil {
		rf.println("Error in unmarshal raft state")
	} else {

		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastApplied = 0
		rf.logEntries = logs
		rf.lastLogs = lastlogs
	}
	rf.unlock("Raft.readPersist")
	rf.logSnapshot.datas = rf.persister.ReadSnapshot()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock("Raft.GetState")
	defer rf.unlock("Raft.GetState")
	isleader := rf.status == Leader
	return rf.currentTerm, isleader
}

// 设置任期
func (rf *Raft) setTerm(term int) {
	rf.lock("Raft.setTerm")
	defer rf.unlock("Raft.setTerm")
	rf.currentTerm = term
}

// 增加任期
func (rf *Raft) addTerm(term int) {
	rf.lock("Raft.addTerm")
	defer rf.unlock("Raft.addTerm")
	rf.currentTerm += term
}

// After the election, set the status for the rf
func (rf *Raft) setStatus(status int) {
	rf.lock("Raft.setStatus")
	defer rf.unlock("Raft.setStatus")
	if (rf.status != Follower) && (status == Follower) {
		rf.resetElectTimer()
	}
	if rf.status != Leader && status == Leader {
		index := len(rf.logEntries)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = index + 1 + rf.logSnapshot.index
			rf.matchIndex[i] = 0
		}
	}
	rf.status = status
}

// 获取状态
func (rf *Raft) getStatus() int {
	rf.lock("Raft.getStatus")
	defer rf.unlock("Raft.getStatus")
	return rf.status
}

// 设置提交日志索引
func (rf *Raft) setCommitIndex(index int) {
	rf.lock("Raft.setCommitIndex")
	defer rf.unlock("Raft.setCommitIndex")
	rf.commitIndex = index
}

// 重置竞选周期定时
func (rf *Raft) resetElectTimer() {
	randCnt := rf.randtime.Intn(250)
	duration := time.Duration(randCnt)*time.Millisecond + CandidateDuration
	rf.eletionTimer.Reset(duration)
}

// 设置最后一次提交
func (rf *Raft) setLastLog(req *AppendEntries) {
	rf.lock("Raft.setLastLog")
	defer rf.unlock("Raft.setLastLog")
	rf.lastLogs = *req
}

// 判断日志顺序是否正确
func (rf *Raft) isOldRequest(req *AppendEntries) bool {
	rf.lock("Raft.isOldRequest")
	defer rf.unlock("Raft.isOldRequest")
	if req.term == rf.lastLogs.term && req.leaderId == rf.lastLogs.leaderId {
		lastIndex := rf.lastLogs.prevLogIndex + rf.lastLogs.snapshot.index + len(rf.lastLogs.entries)
		reqLastIndex := req.prevLogIndex + rf.lastLogs.snapshot.index + len(req.entries)
		return lastIndex > reqLastIndex
	}
	return false
}

// 更新插入同步日志
func (rf *Raft) updateLog(start int, logEntrys []LogEntry, snapshot *LogSnapshot) {
	rf.lock("Raft.updateLog")
	defer rf.unlock("Raft.updateLog")
	if snapshot.index > 0 { //更新快照
		rf.logSnapshot = *snapshot
		start = rf.logSnapshot.index
		rf.println("update snapshot :", rf.me, rf.logSnapshot.index, "len logs", len(logEntrys))
	}
	index := start - rf.logSnapshot.index
	for i := 0; i < len(logEntrys); i++ {
		if index+i < 0 {
			//网络不可靠，fallower节点成功apply并保存快照后，Leader未收到反馈，重复发送日志，
			//可能会导致index <0情况。
			continue
		}
		if index+i < len(rf.logEntries) {
			rf.logEntries[index+i] = logEntrys[i]
		} else {
			rf.logEntries = append(rf.logEntries, logEntrys[i])
		}
	}
	size := index + len(logEntrys)
	if size < 0 { //网络不可靠+各节点独立备份快照可能出现
		size = 0
	}
	//重置log大小
	rf.logEntries = rf.logEntries[:size]
}

// 获取当前日志索引及任期
func (rf *Raft) getLogTermAndIndex() (int, int) {
	rf.lock("Raft.getLogTermAndIndex")
	defer rf.unlock("Raft.getLogTermAndIndex")
	index := 0
	term := 0
	size := len(rf.logEntries)
	if size > 0 {
		index = rf.logEntries[size-1].index
		term = rf.logEntries[size-1].term
	} else {
		index = rf.logSnapshot.index
		term = rf.logSnapshot.term
	}
	return term, index
}

// 根据index获取term
func (rf *Raft) getLogTermOfIndex(index int) int {
	rf.lock("Raft.getLogTermOfIndex")
	defer rf.unlock("Raft.getLogTermOfIndex")
	index -= (1 + rf.logSnapshot.index)
	if index < 0 {
		return rf.logSnapshot.term
	}
	return rf.logEntries[index].term
}

// 获取快照
func (rf *Raft) getSnapshot(index int, snapshot *LogSnapshot) int {
	if index <= rf.logSnapshot.index { //如果follow日志小于快照，则获取快照
		*snapshot = rf.logSnapshot
		index = 0 //更新快照时，从0开始复制日志
	} else {
		index -= rf.logSnapshot.index
	}
	return index
}

// 获取该节点更新日志
func (rf *Raft) getEntriesInfo(index int, snapshot *LogSnapshot, entries *[]LogEntry) (preterm int, preindex int) {
	start := rf.getSnapshot(index, snapshot) - 1
	if start < 0 {
		preindex = 0
		preterm = 0
		start = 0
	} else if start == 0 {
		if rf.logSnapshot.index == 0 {
			preindex = 0
			preterm = 0
		} else {
			preindex = rf.logSnapshot.index
			preterm = rf.logSnapshot.term
		}
	} else {
		preindex = rf.logEntries[start-1].index
		preterm = rf.logEntries[start-1].term
	}
	for i := start; i < len(rf.logEntries); i++ {
		*entries = append(*entries, rf.logEntries[i])
	}
	return
}

// 获取该节点更新日志及信息
func (rf *Raft) getAppendEntries(peer int) AppendEntries {
	rf.lock("Raft.getAppendEntries")
	defer rf.unlock("Raft.getAppendEntries")
	rst := AppendEntries{
		leaderId:     rf.me,
		term:         rf.currentTerm,
		leaderCommit: rf.commitIndex,
		snapshot:     LogSnapshot{index: 0},
	}
	//当前follow的日志状态
	next := rf.nextIndex[peer]
	rst.prevLogTerm, rst.prevLogIndex = rf.getEntriesInfo(next, &rst.snapshot, &rst.entries)
	return rst
}

// 同步日志RPC
func (rf *Raft) sendAppendEnteries(server int, req *AppendEntries, resp *RespEntries) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequeappstAppendEntries", req, resp)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(time.Millisecond * 400):
	}
	return ok
}

func (rf *Raft) RequestAppendEntries(req *AppendEntries, resp *RespEntries) {
	currentTerm, _ := rf.GetState()
	resp.term = currentTerm
	resp.successed = true
	if req.term < currentTerm {
		//leader任期小于自身任期，则拒绝同步log
		resp.successed = false
		return
	}
	//乱序日志，不处理
	if rf.isOldRequest(req) {
		return
	}
	//否则更新自身任期，切换自生为fallow，重置选举定时器
	rf.resetElectTimer()
	rf.setTerm(req.term)
	rf.setStatus(Follower)
	_, logindex := rf.getLogTermAndIndex()
	//判定与leader日志是一致
	if req.prevLogIndex > 0 {
		if req.prevLogIndex > logindex {
			//没有该日志，则拒绝更新
			//rf.println(rf.me, "can't find preindex", req.PrevLogTerm)
			resp.successed = false
			resp.lastApplied = rf.lastApplied
			return
		}
		if rf.getLogTermOfIndex(req.prevLogIndex) != req.prevLogTerm {
			//该索引与自身日志不同，则拒绝更新
			//rf.println(rf.me, "term error", req.PrevLogTerm)
			resp.successed = false
			resp.lastApplied = rf.lastApplied
			return
		}
	}
	//更新日志
	rf.setLastLog(req)
	if len(req.entries) > 0 || req.snapshot.index > 0 {
		if len(req.entries) > 0 {
			rf.println(rf.me, "update log from ", req.leaderId, ":", req.entries[0].term, "-", req.entries[0].index, "to", req.entries[len(req.entries)-1].term, "-", req.entries[len(req.entries)-1].index)
		}
		rf.updateLog(req.prevLogIndex, req.entries, &req.snapshot)
	}
	rf.setCommitIndex(req.leaderCommit)
	rf.apply()
	rf.persist()

	return
}

// 选举定时器loop
func (rf *Raft) ElectionLoop() {
	//选举超时定时器
	rf.resetElectTimer()
	defer rf.eletionTimer.Stop()

	for !rf.dead {
		<-rf.eletionTimer.C
		if rf.dead {
			break
		}
		if rf.getStatus() == Candidate {
			//如果状态为竞选者，则直接发动投票
			rf.resetElectTimer()
			rf.Vote()
		} else if rf.getStatus() == Follower {
			//如果状态为follow，则转变为candidata并发动投票
			rf.setStatus(Candidate)
			rf.resetElectTimer()
			rf.Vote()
		}
	}
	rf.println(rf.me, "Exit ElectionLoop")
}

// 收到投票请求
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	reply.voteGranted = true
	reply.term, _ = rf.GetState()
	//竞选任期小于自身任期，则反对票
	if reply.term >= req.term {
		fmt.Printf("RV: %v\n", req)
		reply.voteGranted = false
		return
	}
	//竞选任期大于自身任期，则更新自身任期，并转为follow
	rf.setStatus(Follower)
	rf.setTerm(req.term)
	logterm, logindex := rf.getLogTermAndIndex()
	//判定竞选者日志是否新于自己
	if logterm > req.lastLogTerm {
		reply.voteGranted = false
	} else if logterm == req.lastLogTerm {
		reply.voteGranted = logindex <= req.lastLogIndex
		if !reply.voteGranted {
			rf.println(rf.me, "refuse", req.candidateId, "because of logs's index")
		}
	}
	if reply.voteGranted {
		rf.println(rf.me, "agree", req.candidateId)
		//赞同票后重置选举定时，避免竞争
		rf.resetElectTimer()
	}
}

func (rf *Raft) Vote() {
	//投票先增大自身任期
	rf.addTerm(1)
	rf.println("start vote :", rf.me, "term :", rf.currentTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	currentTerm, _ := rf.GetState()
	req := RequestVoteArgs{
		candidateId:  rf.me,
		term:         currentTerm,
		lastLogTerm:  logterm,
		lastLogIndex: logindex,
	}
	var wait sync.WaitGroup
	peercnt := len(rf.peers)
	wait.Add(peercnt)
	agreeVote := 0
	term := currentTerm
	for i := 0; i < peercnt; i++ {
		//并行调用投票rpc，避免单点阻塞
		go func(index int) {
			defer wait.Done()
			resp := RequestVoteReply{false, -1}
			//给自己投票
			if index == rf.me {
				agreeVote++
				return
			}
			//请求别的sever投票
			fmt.Printf("V: %v\n", req)
			rst := rf.sendRequestVote(index, &req, &resp)
			if !rst {
				return
			}
			if resp.voteGranted {
				agreeVote++
				return
			}
			if resp.term > term {
				term = resp.term
			}
		}(i)
	}
	wait.Wait()
	//如果存在系统任期更大，则更新任期并转为fallow
	if term > currentTerm {
		rf.setTerm(term)
		rf.setStatus(Follower)
	} else if agreeVote*2 > peercnt { //获得多数赞同则变成leader
		rf.println(rf.me, "become leader :", currentTerm)
		rf.setStatus(Leader)
		rf.replicateLogNow()
	}
}

// 设置follower next和match日志索引
func (rf *Raft) setNextAndMatch(peer int, index int) {
	rf.lock("Raft.setNextAndMatch")
	defer rf.unlock("Raft.setNextAndMatch")
	rf.nextIndex[peer] = index + 1
	rf.matchIndex[peer] = index
}

// 减小nextIndex寻找follower和leader匹配的log
func (rf *Raft) setNextIndex(peer int, next int) {
	rf.lock("Raft.decNextIndex")
	defer rf.unlock("Raft.decNextIndex")
	rf.nextIndex[peer] = next
}

// 复制日志给follower
func (rf *Raft) replicateLogTo(peer int) bool {
	replicateRst := false
	if peer == rf.me {
		return replicateRst
	}
	isLoop := true
	for isLoop {
		isLoop = false
		currentTerm, isLeader := rf.GetState()
		if !isLeader || rf.dead {
			break
		}
		req := rf.getAppendEntries(peer)
		resp := RespEntries{term: 0}
		rst := rf.sendAppendEnteries(peer, &req, &resp)
		currentTerm, isLeader = rf.GetState()
		if rst && isLeader {
			//如果某个节点任期大于自己，则更新任期，变成follow
			if resp.term > currentTerm {
				rf.println(rf.me, "become fallow ", peer, "term :", resp.term)
				rf.setTerm(resp.term)
				rf.setStatus(Follower)
				// false if log doesnot contain an entry at prevLogIndex whose term matches prevLogTerm
			} else if !resp.successed { //如果更新失败则更新follow日志next索引
				//rf.incNext(peer)
				rf.setNextIndex(peer, resp.lastApplied+1)
				isLoop = true
			} else { //更新成功
				if len(req.entries) > 0 {
					rf.setNextAndMatch(peer, req.entries[len(req.entries)-1].index)
					replicateRst = true
				} else if req.snapshot.index > 0 {
					rf.setNextAndMatch(peer, req.snapshot.index)
					replicateRst = true
				}
			}
		} else {
			isLoop = true
		}
	}
	return replicateRst
}

// 立即复制日志
func (rf *Raft) replicateLogNow() {
	rf.lock("Raft.replicateLogNow")
	defer rf.unlock("Raft.replicateLogNow")
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i].Reset(0)
	}
}

// 心跳周期复制日志给peer
func (rf *Raft) ReplicateLogLoop(peer int) {
	defer func() {
		rf.heartbeatTimers[peer].Stop()
	}()
	for !rf.dead {
		<-rf.heartbeatTimers[peer].C
		if rf.dead {
			break
		}
		rf.lock("Raft.ReplicateLogLoop")
		rf.heartbeatTimers[peer].Reset(HeartbeatDuration)
		rf.unlock("Raft.ReplicateLogLoop")
		_, isLeader := rf.GetState()
		if isLeader {
			success := rf.replicateLogTo(peer)
			if success {
				rf.apply()
				rf.replicateLogNow()
				rf.persist()
			}
		}
	}
	rf.println(rf.me, "-", peer, "Exit ReplicateLogLoop")
}

// 获取当前已被提交日志
func (rf *Raft) updateCommitIndex() bool {
	rst := false
	var indexs []int
	if len(rf.logEntries) > 0 {
		rf.matchIndex[rf.me] = rf.logEntries[len(rf.logEntries)-1].index
	} else {
		rf.matchIndex[rf.me] = rf.logSnapshot.index
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		indexs = append(indexs, rf.matchIndex[i])
	}
	//大多数peer已经match的index可被commit
	sort.Ints(indexs)
	index := len(indexs) / 2
	commit := indexs[index]
	if commit > rf.commitIndex {
		rf.println(rf.me, "update leader commit index", commit)
		rst = true
		rf.commitIndex = commit
	}
	return rst
}

// apply 状态机
func (rf *Raft) apply() {
	rf.lock("Raft.apply")
	defer rf.unlock("Raft.apply")
	if rf.status == Leader {
		rf.updateCommitIndex()
	}

	lastapplied := rf.lastApplied
	if rf.lastApplied < rf.logSnapshot.index {
		msg := ApplyMsg{
			CommandValid: false,
			Command:      rf.logSnapshot,
			CommandIndex: 0,
		}
		rf.applyCh <- msg
		rf.lastApplied = rf.logSnapshot.index
		rf.println(rf.me, "apply snapshot :", rf.logSnapshot.index, "with logs:", len(rf.logEntries))
	}
	last := 0
	if len(rf.logEntries) > 0 {
		last = rf.logEntries[len(rf.logEntries)-1].index
	}
	for ; rf.lastApplied < rf.commitIndex && rf.lastApplied < last; rf.lastApplied++ {
		index := rf.lastApplied
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[index-rf.logSnapshot.index].log,
			CommandIndex: rf.logEntries[index-rf.logSnapshot.index].index,
		}
		rf.applyCh <- msg
	}
	if rf.lastApplied > lastapplied {
		numAppliedIndex := rf.lastApplied - 1 - rf.logSnapshot.index
		endIndex, endTerm := 0, 0
		if numAppliedIndex < 0 {
			endIndex = rf.logSnapshot.index
			endTerm = rf.logSnapshot.term
		} else {
			endTerm = rf.logEntries[rf.lastApplied-1-rf.logSnapshot.index].term
			endIndex = rf.logEntries[rf.lastApplied-1-rf.logSnapshot.index].index
		}
		rf.println(rf.me, "apply log", rf.lastApplied-1, endTerm, "-", endIndex, "/", last)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = 0
	term, isLeader = rf.GetState()
	if isLeader {
		index = rf.insertLog(command)
		rf.replicateLogNow()
	}

	// Your code here (2B).

	return
}

// 插入日志
func (rf *Raft) insertLog(command interface{}) int {
	rf.lock("Raft.insertLog")
	defer rf.unlock("Raft.insertLog")
	entry := LogEntry{
		term:  rf.currentTerm,
		index: 1,
		log:   command,
	}
	//获取log索引
	if len(rf.logEntries) > 0 {
		entry.index = rf.logEntries[len(rf.logEntries)-1].index + 1
	} else {
		entry.index = rf.logSnapshot.index + 1
	}
	//插入log
	rf.logEntries = append(rf.logEntries, entry)
	return entry.index
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.dead = true
	rf.eletionTimer.Reset(0)
	rf.replicateLogNow()
	// Your code here, if desired.
}

// func (rf *Raft) killed() bool {
// 	z := atomic.LoadInt32(&rf.dead)
// 	return z == 1
// }

// 保存快照
func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.lock("Raft.SaveSnapshot")
	if index > rf.logSnapshot.index {
		//保存快照
		start := rf.logSnapshot.index
		rf.logSnapshot.index = index
		rf.logSnapshot.datas = snapshot
		rf.logSnapshot.term = rf.logEntries[index-start-1].term
		//删除快照日志
		if len(rf.logEntries) > 0 {
			rf.logEntries = rf.logEntries[(index - start):]
		}
		rf.println("save snapshot :", rf.me, index, ",len logs:", len(rf.logEntries))
		rf.unlock("Raft.SaveSnapshot")
		rf.persist()
	} else {
		rf.unlock("Raft.SaveSnapshot")
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.randtime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.dead = false
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.eletionTimer = time.NewTimer(CandidateDuration)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.setStatus(Follower)
	rf.EnableDebugLog = false
	rf.lastLogs = AppendEntries{
		leaderId: -1,
		term:     -1,
	}
	rf.logSnapshot = LogSnapshot{
		index: 0,
		term:  0,
	}

	// Your initialization code here (2A, 2B, 2C).
	//日志同步协程
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		go rf.ReplicateLogLoop(i)
	}
	//initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.apply()
	raftOnce.Do(func() {
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	})
	//Leader选举协程
	go rf.ElectionLoop()

	return rf
}
