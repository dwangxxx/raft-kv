package kvdb

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"

	"wang.deng/raft-kv/rpcutil"
	"github.com/satori/go.uuid"
)
import "crypto/rand"
import "math/big"

type ClientConfig struct {
	ClientEnd []struct {
		Ip   string
		Port string
	} `yaml:"servers"`
}

type Clerk struct {
	servers []*rpcutil.ClientEnd
	id      uuid.UUID
	servlen int
	leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*rpcutil.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = generateUUID()
	ck.servlen = len(servers)
	return ck
}

func (ck *Clerk) Get(key string) string {

	args := &GetArgs{
		Key:    key,
		Id:     ck.id,
		Serial: generateUUID(),
	}
	reply := &GetReply{}
	DPrintf("%v 发送 Get 请求 {Key=%v Serial=%v}",
		ck.id, key, args.Serial)
	for {
		if ok := ck.servers[ck.leader].Call(RPCGet, args, reply); !ok {
			DPrintf("%v 对 服务器 %v 的 Get 请求 (Key=%v Serial=%v) 超时",
				ck.id, ck.leader, key, args.Serial)
			// 切换领导者
			ck.leader = (ck.leader + 1) % ck.servlen
			continue
		}
		if reply.Err == OK {
			DPrintf("%v 收到对 %v 发送的 Get 请求 {Key=%v Serial=%v} 的响应，结果为 %v",
				ck.id, ck.leader, key, args.Serial, reply.Value)
			return reply.Value
		} else if reply.Err == ErrNoKey {
			DPrintf("%v 收到对 %v 发送的 Get 请求 {Key=%v Serial=%v} 的响应，结果为 ErrNoKey",
				ck.id, ck.leader, key, args.Serial)
			return NoKeyValue
		} else if reply.Err == ErrWrongLeader {
			DPrintf("错误的领导者")
			// 请求了错误的领导者，循环请求领导者
			ck.leader = (ck.leader + 1) % ck.servlen
			// ck.leader = 0
			continue
		} else {
			panic(fmt.Sprintf("%v 对 服务器 %v 的 Get 请求 (Key=%v Serial=%v) 收到一条空 Err",
				ck.id, ck.leader, key, args.Serial))
		}
	}
}

func (ck *Clerk) putAppend(key string, value string, op string) {
	args := &PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Id:     ck.id,
		Serial: generateUUID(),
	}
	reply := &PutAppendReply{}
	DPrintf("%v 发送 PA 请求 {Op=%v Key=%v Value='%v' Serial=%v}",
		ck.id, op, key, value, args.Serial)
	for {
		if ok := ck.servers[ck.leader].Call(RPCPutAppend, args, reply); !ok {
			//DPrintf("%v 对 服务器 %v 的 PutAppend 请求 (Serial=%v Key=%v Value=%v op=%v) 超时",
			//	ck.id, ck.leader, args.Serial, key, value, op)
			ck.leader = (ck.leader + 1) % ck.servlen
			continue
		}
		if reply.Err == OK {
			//DPrintf("%v 收到对 %v 发送的 PA 请求 {Op=%v Key=%v Value='%v' Serial=%v} 的响应，结果为 OK",
			//	ck.id, ck.leader, op, key, value, args.Serial)
			return
		} else if reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % ck.servlen
			continue
		} else {
			panic(fmt.Sprintf("%v 对 服务器 %v 的 PutAppend 请求 (Serial=%v Key=%v Value=%v op=%v) 收到一条空 Err",
				ck.id, ck.leader, args.Serial, key, value, op))
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.putAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.putAppend(key, value, OpAppend)
}

// 获取客户端通信实例
func GetClientEnds(path string) []*rpcutil.ClientEnd {
	config := getClientConfig(path)
	num := len(config.ClientEnd)
	if (num & 1) == 0 || num < 3 {
		panic("the number of servers must be odd and greater than or equal to 3")
	}

	clientEnds := make([]*rpcutil.ClientEnd, 0)
	for _, end := range config.ClientEnd {
		address := end.Ip + ":" + end.Port
		client := rpcutil.TryConnect(address)

		ce := &rpcutil.ClientEnd{
			Addr:   address,
			Client: client,
		}

		clientEnds = append(clientEnds, ce)
	}
	return clientEnds
}

func getClientConfig(path string) *ClientConfig {
	if len(os.Args) == 2 {
		path = os.Args[1]
	}

	cfgbt, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	config := &ClientConfig{}
	err = yaml.Unmarshal(cfgbt, config)
	if err != nil {
		panic(err)
	}
	return config
}

func generateUUID() uuid.UUID {
	id := uuid.NewV1()
	return id
}
