package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync/atomic"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	leaderId  int
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	req := GetArgs{
		Key: key,
	}
	for i := 0; i < 8000; i++ {
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &req, &reply)
		if ok && !reply.wrongLeader {
			time.Sleep(time.Millisecond * 100)
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 5)
	}
	log.Fatalln("Get", key, "timeout")
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	req := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		clientId:    ck.clientId,
		sequenceNum: atomic.AddInt64(&ck.commandId, 1),
	}
	for i := 0; i < 8000; i++ {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &req, &reply)
		if ok && !reply.wrongLeader {
			time.Sleep(time.Millisecond * 100)
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(time.Millisecond * 5)
	}
	log.Fatalln(op, key, value, "timeout")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
