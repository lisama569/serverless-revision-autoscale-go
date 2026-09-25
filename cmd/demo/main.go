// demo 在本地演示：版本发布、流量切换、背压、优雅排空与缩容到零。
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	fc "github.com/lisama569/serverless-revision-autoscale-go/fc"
)

func main() {
	logger := func(format string, args ...any) { log.Printf(format, args...) }
	f := fc.NewFunction("hello", logger)
	ctx := context.Background()

	// 1. 发布 v1 并承接全部流量。
	v1, err := f.Publish(func(ctx context.Context, req *fc.Request) (string, error) {
		return "hello from v1", nil
	}, fc.RevisionConfig{IdleTimeout: 200 * time.Millisecond}, map[fc.RevisionID]int{"": 1}, f.Epoch())
	must(err)

	resp, _ := f.Invoke(ctx, &fc.Request{Payload: "ping"})
	fmt.Printf("=> %q via %s on %s\n\n", resp.Body, resp.Revision, resp.InstanceID)

	// 2. 发布 v2，灰度 50/50，再整体切到 v2。
	v2, err := f.Publish(func(ctx context.Context, req *fc.Request) (string, error) {
		time.Sleep(50 * time.Millisecond) // 模拟慢请求，便于观察排空
		return "hello from v2", nil
	}, fc.RevisionConfig{}, map[fc.RevisionID]int{"": 1, v1: 1}, f.Epoch())
	must(err)

	for i := 0; i < 4; i++ {
		resp, _ := f.Invoke(ctx, &fc.Request{Payload: "canary"})
		fmt.Printf("=> canary %d -> %s\n", i, resp.Revision)
	}
	must(f.UpdateTraffic(map[fc.RevisionID]int{v2: 1}, f.Epoch()))
	fmt.Println("=> traffic fully shifted to v2; v1 draining in background")

	// 3. 无流量缩容到零，随后冷启动。
	time.Sleep(500 * time.Millisecond)
	f.Tick()
	fmt.Printf("=> stats after idle: %+v\n", f.Stats())
	resp, _ = f.Invoke(ctx, &fc.Request{Payload: "cold-start"})
	fmt.Printf("=> cold start on %s\n", resp.InstanceID)

	f.Close()
	fmt.Println("=> all instances drained, demo done")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
