package main

import (
	"bufio"
	"log"
	"os"
	"strings"
	"wang.deng/raft-kv/kvdb"
)

type Command struct {
	Name   string
	Num    int
	Action func(args []string)
}

const (
	EXIT   = "exit"
	TIP    = "> "
	GET    = "get"
	APPEND = "append"
	PUT    = "put"
	ILLAGM = "illegal argument"
	// 指示客户端的命令
	HUMEN  = `Commands:
	"get k"
	"append k v"
	"put k v"
	"exit"
`
)

func main() {
	clientEnds := kvdb.GetClientEnds("example/config/client.yml")

	clerk := kvdb.MakeClerk(clientEnds)

	// 命令的种类
	commands := []*Command{
		{
			// GET命令
			Name: GET,
			Num:  2,	// 参数个数
			Action: func(args []string) {
				println(clerk.Get(args[1]))
			},
		},
		{
			Name: APPEND,
			Num:  3,
			Action: func(args []string) {
				clerk.Append(args[1], args[2])
			},
		},
		{
			Name: PUT,
			Num:  3,
			Action: func(args []string) {
				clerk.Put(args[1], args[2])
			},
		},
	}

	// 输出命令格式
	println(HUMEN)
	// 循环读取命令行的命令
	for line := readLine(); !strings.EqualFold(line, EXIT); {
		// 将从命令行读取的数据参数进行解析, 解析具体的参数
		args := parseArgs(line)
		if len(args) > 0 {
			name := args[0]
			do := false
			// 遍历commands种类, 匹配具体的命令
			for _, command := range commands {
				if command.Name != name {
					continue
				}

				do = true
				if len(args) != command.Num {
					println(ILLAGM + HUMEN)
					continue
				}
				command.Action(args)
			}

			if !do {
				println(HUMEN)
			}
		}

		// 从命令行读取数据
		line = readLine()
	}
}

// 解析命令行参数
func parseArgs(line string) []string {
	// 将参数按照空格进行划分
	argsT := strings.Split(line, " ")
	args := make([]string, 0)
	// 将命令添加到返回参数args中
	for _, str := range argsT {
		if !strings.EqualFold(str, " ") && !strings.EqualFold(str, "") {
			args = append(args, str)
		}
	}
	return args
}

// 从命令行读取具体的命令
func readLine() string {
	// 创建一个IO读写缓存
	reader := bufio.NewReader(os.Stdin)
	print(TIP)
	// 从命令行读取命令
	answer, _, err := reader.ReadLine()
	if err != nil {
		log.Fatal(err)
	}
	return strings.TrimSpace(string(answer))
}
