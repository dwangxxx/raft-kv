package kvdb

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"wang.deng/raft-kv/labgob"
	"wang.deng/raft-kv/raft"
	"wang.deng/raft-kv/rpcutil"
	uuid "github.com/satori/go.uuid"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// command定义
type Op struct {
	Type   string
	Key    string
	Value  string
	Serial uuid.UUID
}

type CommonReply struct {
	Err    Err
	Key    string
	Value  string
	Serial *uuid.UUID
}

type KVServer struct {
	mu      sync.Mutex
	me      int	// 当前节点的索引号
	rf      *raft.Raft	// 当前节点对应的Raft实例
	applyCh chan raft.ApplyMsg	// Apply通道，用来执行命令
	dead    int32

	maxraftstate int // 如果日志变得很长，就备份一份快照

	data          map[string]string	// 存储的数据
	commonReplies []*CommonReply	// 命令执行的返回值
}

// Get接口, 供Client进行RPC调用
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	op := &Op{
		Type:   OpGet,
		Key:    args.Key,
		Value:  NoKeyValue,
		Serial: args.Serial,
	}
	reply.Err = ErrWrongLeader
	// 调用Raft节点的start接口执行具体的操作
	idx, _, isLeader := kv.rf.Start(*op)
	// 如果不是leader, 就返回
	if !isLeader {
		DPrintf("%v 对于 %v 的 Get 请求 {Key=%v Serial=%v} 处理结果为 该服务器不是领导者",
			kv.me, args.Id, args.Key, args.Serial)
		return nil
	}
	fmt.Printf("%v 等待对 %v 的 Get 请求 {Key=%v Serial=%v} 的提交，应提交索引为 %v\n",
		kv.me, args.Id, args.Key, args.Serial, idx)

	// Raft节点日志执行完毕, Apply到数据库进行具体的操作
	commonReply := &CommonReply{}
	// 在commonReplies中查找对应的reply
	find := kv.findReply(op, idx, commonReply)
	// 如果找到就设置对应的reply.Value
	if find == OK {
		reply.Value = commonReply.Value
		reply.Err = commonReply.Err
	}
	fmt.Printf("%v 对于 %v 的 Get 请求 {Key=%v Serial=%v} 处理结果为 %v, len(Value)=%v\n",
		kv.me, args.Id, args.Key, args.Serial, reply.Err, len(reply.Value))

	return nil
}

// PutAppend接口, 供client进行RPC通信
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	op := &Op{
		Type:   args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Serial: args.Serial,
	}
	reply.Err = ErrWrongLeader
	// kv.rf.Strat(*op)函数执行具体的命令
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		DPrintf("%v 对于 %v 的 %v 请求 {Key=%v Serial=%v Value='%v'} 处理结果为 该服务器不是领导者",
			kv.me, args.Id, args.Op, args.Key, args.Serial, args.Value)
		return nil
	}
	fmt.Printf("%v 等待对 %v 的 %v 请求 {Key=%v Serial=%v Value='%v'} 的提交，应提交索引为 %v\n",
		kv.me, args.Id, args.Op, args.Key, args.Serial, args.Value, idx)

	commonReply := &CommonReply{}
	// 查找对应的commonReply
	find := kv.findReply(op, idx, commonReply)
	if find == OK {
		reply.Err = commonReply.Err
	}
	fmt.Printf("%v 对于 %v 的 %v 请求 {Key=%v Serial=%v Value='%v'} 处理结果为 %v\n",
		kv.me, args.Id, args.Op, args.Key, args.Serial, args.Value, reply.Err)

	return nil
}

func (kv *KVServer) findReply(op *Op, idx int, reply *CommonReply) string {
	t0 := time.Now()
	// 设置一个超时时间, 如果超过这个时间表示提交失败(超时)
	for time.Since(t0).Seconds() < 2 {
		kv.mu.Lock()
		if len(kv.commonReplies) > idx {
			// 根据idx来获取对应的commonReply数据
			if op.Serial == *kv.commonReplies[idx].Serial {
				reply1 := kv.commonReplies[idx]
				reply.Err = reply1.Err
				reply.Key = reply1.Key
				reply.Value = reply1.Value
				kv.mu.Unlock()
				return OK
			} else {
				kv.mu.Unlock()
				return CommitTimeout
			}
		}
		kv.mu.Unlock()
		// sleep一定时间, 等待Apply完成
		time.Sleep(20 * time.Millisecond)
	}
	return CommitTimeout
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// server的入口函数, 此处创建一个KVServer实例，然后启动一个协程来做Apply到数据库的一些操作
func StartKVServer(servers []*rpcutil.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// 新建一个KVServer实例
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// 新建一个ApplyMsg通道, 用于Apply命令的传送
	kv.applyCh = make(chan raft.ApplyMsg)
	// 调用Make函数来创建一个Raft节点实例(Make函数中会创建几个goroutine来进行leader选举以及日志的复制)
	kv.rf = raft.MakeRaft(servers, me, persister, kv.applyCh)
	// 存放数据的字典
	kv.data = make(map[string]string)
	// 命令执行的返回值
	kv.commonReplies = make([]*CommonReply, 1)

	// 调用Register函数注册RPC函数
	if err := rpc.Register(kv.rf); err != nil {
		panic(err)
	}

	// 创建一个协程来处理Apply到数据库的操作
	go kv.opHandler()

	return kv
}

func (kv *KVServer) opHandler() {
	// for循环不断进行操作
	for {
		if kv.killed() {
			return
		}

		// 从applyCh通道中获取Apply的命令
		// 如果通道里面没有数据了, 就会阻塞住
		applyMsg := <-kv.applyCh
		// 获取具体的命令
		op := applyMsg.Command.(Op)
		reply := &CommonReply{
			Key:    op.Key,
			Value:  op.Value,
			Serial: &op.Serial,
		}
		if op.Type == OpPut {	// put key value操作
			kv.data[op.Key] = op.Value
			reply.Err = OK
		} else if op.Type == OpAppend {	// append key value操作
			if _, has := kv.data[op.Key]; has {
				kv.data[op.Key] += op.Value
			} else {
				kv.data[op.Key] = op.Value
			}
			reply.Err = OK
		} else if op.Type == OpGet {	// get key操作
			if value, ok := kv.data[op.Key]; ok {
				reply.Value = value
				reply.Err = OK
			} else {	// 不存在key
				reply.Err = ErrNoKey
			}
		} else {
			panic(fmt.Sprintf("%v: Get 命令中 op.Type 的值 %v 错误，附 op 的值 {Key=%v Value=%v}",
				kv.me, op.Type, op.Key, op.Value))
		}

		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			// 将对应的reply加到commonReplies后面
			kv.commonReplies = append(kv.commonReplies, reply)
		}()
	}
}
