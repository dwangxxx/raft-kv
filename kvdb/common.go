package kvdb

import uuid "github.com/satori/go.uuid"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	NoKeyValue     = ""
	RPCGet         = "KVServer.Get"
	RPCPutAppend   = "KVServer.PutAppend"
	OpPut          = "Put"
	OpAppend       = "Append"
	OpGet          = "Get"
	CommitTimeout  = "CommitTimeout"
)

type Err string

// PutAppend的请求参数
type PutAppendArgs struct {
	Key    string
	Value  string
	Op     string // "Put" or "Append"
	Id     uuid.UUID
	Serial uuid.UUID
}

// PutAppend的返回参数
type PutAppendReply struct {
	Err Err
}

// Get命令的请求参数(只有一个Key)
type GetArgs struct {
	Key    string
	Id     uuid.UUID
	Serial uuid.UUID
}

// Get命令的返回参数, 返回key对应的value
type GetReply struct {
	Err   Err
	Value string
}
