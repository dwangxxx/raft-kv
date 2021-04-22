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

	commands := []*Command{
		{
			Name: GET,
			Num:  2,
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

	println(HUMEN)
	for line := readLine(); !strings.EqualFold(line, EXIT); {
		args := parseArgs(line)
		if len(args) > 0 {
			name := args[0]
			do := false
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

		line = readLine()
	}
}

func parseArgs(line string) []string {
	argsT := strings.Split(line, " ")
	args := make([]string, 0)
	for _, str := range argsT {
		if !strings.EqualFold(str, " ") && !strings.EqualFold(str, "") {
			args = append(args, str)
		}
	}
	return args
}

func readLine() string {
	reader := bufio.NewReader(os.Stdin)
	print(TIP)
	answer, _, err := reader.ReadLine()
	if err != nil {
		log.Fatal(err)
	}
	return strings.TrimSpace(string(answer))
}
