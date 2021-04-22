package rpcutil

import (
	"log"
	"net/rpc"
)

// 客户端实例, Addr是对端地址, Client是内置rpc的的Client
type ClientEnd struct {
	Addr   string
	// 与对端通信的rpc工具
	Client *rpc.Client
}

// 使用这个函数来进行RPC通信, 请求运行对端名为methodName的函数, args是函数参数, reply是函数返回值
func (e *ClientEnd) Call(methodName string, args interface{}, reply interface{}) bool {
	// 如果client为空，则首先创建一个rpc.client实例
	if e.Client == nil {
		// 首先创建一个client实例
		e.Client = TryConnect(e.Addr)
		if e.Client == nil {
			return false
		}
	}

	// 使用client来进行RPC的call通信, 在client上运行名为methodName的函数
	// args参数是methodName的参数，reply是返回的数据
	err := e.Client.Call(methodName, args, reply)
	// 如果返回的err不是空指针nil, 则表示发生了某种错误
	if err != nil {
		log.Println(err)
		e.Client = nil
		return false
	}
	// 执行正确, 返回true
	return true
}

// 进行tcp连接尝试
func TryConnect(address string) *rpc.Client {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println(err)
		return nil
	}

	return client
}
