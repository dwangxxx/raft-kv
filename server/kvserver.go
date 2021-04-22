package main

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"wang.deng/raft-kv/kvdb"
	"wang.deng/raft-kv/raft"
	"wang.deng/raft-kv/rpcutil"
)

// 配置文件结构体Config
type Config struct {
	// 服务器的Ip和对应的Port
	Servers []struct {
		Ip   string
		Port string
	}
	Me int `yaml:"me"`
}

func main() {
	// 首先解析cfg文件
	serverCfg := getCfg()
	i := len(serverCfg.Servers)
	if (i & 1) == 0 {
		panic("总服务器数量必须为单数")
	}

	// 获取集群的所有服务器列表
	clientEnds := getClientEnds(serverCfg)
	// 持久化工具
	persister := raft.MakePersister()
	// 获取一个kvserver实例
	kvur := kvdb.StartKVServer(clientEnds, serverCfg.Me, persister, i)

	// 首先将kvur进行注册，注册之后的方法才可以进行RPC调用
	// 使用rpc.Register进行注册
	if err := rpc.Register(kvur); err != nil {
		panic(err)
	}

	// 注册 HTTP 路由
	rpc.HandleHTTP()
	// 监听端口
	l, e := net.Listen("tcp", ":" + serverCfg.Servers[serverCfg.Me].Port)
	if e != nil {
		panic(e)
	}
	log.Println("Listen", serverCfg.Servers[serverCfg.Me].Port)

	// 启动一个Http服务，请求rpc的方法会交给rpc内部路由进行处理
	http.Serve(l, nil)
}

func getClientEnds(serverCfg Config) []*rpcutil.ClientEnd {
	// 创建clientEnds数组，初始长度为0
	clientEnds := make([]*rpcutil.ClientEnd, 0)
	// 遍历所有server
	for i, end := range serverCfg.Servers {
		// server的地址: IP:PORT
		address := end.Ip + ":" + end.Port
		var client *rpc.Client
		if i == serverCfg.Me {
			client = nil
		} else {
			// 如果不是自身，则尝试与别的服务器进行进行连接
			client = rpcutil.TryConnect(address)
		}

		// 新建一个ClientEnd服务器
		tmpClientEnd := &rpcutil.ClientEnd{
			Addr: address,
			Client: client,
		}

		clientEnds = append(clientEnds, tmpClientEnd)
	}
	return clientEnds
}

func getCfg() Config {
	var path string
	// 如果没有指定config文件的地址，则使用默认的config文件
	if len(os.Args) == 1 {
		path = "config/server.yaml"
	} else {
		path = os.Args[1]
	}

	// 读取文件
	cfgFile, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	config := Config{}
	// 使用外部库解析yaml文件
	err = yaml.Unmarshal(cfgFile, &config)
	if err != nil {
		panic(err)
	}

	return config
}
