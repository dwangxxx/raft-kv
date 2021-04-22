package main

import (
	"fmt"
	"wang.deng/raft-kv/kv"
)

func main() {
	clientEnds := kv.GetClientEnds("example/config/client.yml")

	clerk := kv.MakeClerk(clientEnds)

	fmt.Printf("k1=%v\n", clerk.Get("k1"))

	clerk.Put("k1", "3")
	fmt.Printf("k1=%v\n", clerk.Get("k1"))

	clerk.Append("k1", "4")
	fmt.Printf("k1=%v\n", clerk.Get("k1"))
}
