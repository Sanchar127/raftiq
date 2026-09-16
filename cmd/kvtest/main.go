package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/client"
)

func main() {
	c, err := client.Dial("127.0.0.1:8001")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Put(ctx, "grafana-test", []byte("hello")); err != nil {
		panic(err)
	}

	value, found, err := c.Get(ctx, "grafana-test")
	if err != nil {
		panic(err)
	}

	fmt.Printf("found=%v value=%q\n", found, value)
}
