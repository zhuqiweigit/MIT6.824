package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"../labrpc"
)

const (
	RetrySendReqTimeMs = time.Millisecond * 20
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64
	operationId int64
	leaderId    int
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
	ck.clientId = nrand()
	ck.operationId = 0
	ck.leaderId = 0
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	request := GetArgs{Key: key, ClientId: ck.clientId, OperationId: ck.operationId}
	//fmt.Printf("request Put %+v\n", request)
	ck.operationId++
	for {
		response := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &request, &response)

		if ok == false {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(RetrySendReqTimeMs)
			continue
		}

		if response.Err == ErrNoKey {
			return ""
		} else if response.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if response.Err == ErrRaftTimeout {
			//超时，maybe定位到了partition的旧leader上
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if response.Err == OK {
			return response.Value
		} else {
			log.Fatal("wrong GET reply args")
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	request := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, OperationId: ck.operationId}
	//fmt.Printf("request PutAppend %+v\n", request)
	ck.operationId++
	for {
		response := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &request, &response)

		if ok == false {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(RetrySendReqTimeMs)
			continue
		}
		if response.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if response.Err == ErrRaftTimeout {
			//超时，maybe定位到了partition的旧leader上
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		} else if response.Err == OK {
			break
		} else {
			log.Fatal("PutAppend reponse args error")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
