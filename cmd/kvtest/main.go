package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/client"
)

func main() {
	c, err := client.Dial("127.0.0.1:8002")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Put(ctx, "grafana-test", []byte("hello")); err != nil {
		panic(err)
	}

	value, found, err := c.Get(ctx, "grafana-test")
	if err != nil {
		panic(err)
	}

	fmt.Printf("KV: found=%v value=%q\n", found, value)

	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())

	index, err := c.CreateJob(
		ctx,
		jobID,
		[]byte("hello from raftiq worker"),
		0,
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf(
		"JOB CREATED: id=%s index=%d\n",
		jobID,
		index,
	)

	fmt.Println("Waiting for scheduler and worker execution...")
	time.Sleep(2 * time.Second)

	fmt.Printf(
		"JOB SUBMISSION COMPLETE: id=%s\n",
		jobID,
	)
}
