// demo 在本地演示：版本发布、渐进放量、粘性路由、保底常驻实例、
// 流量切换、背压、优雅排空与缩容到零。
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

	// 2. 发布 v2（保底 1 个常驻实例，灰度期间无流量也不冷启动），
	//    启动渐进放量：10% -> 50% -> 100%，每步间隔 4 次请求。
	v2, err := f.Publish(func(ctx context.Context, req *fc.Request) (string, error) {
		return "hello from v2", nil
	}, fc.RevisionConfig{MinInstances: 1}, nil, f.Epoch())
	must(err)
	steps := []fc.RolloutStep{{Percent: 10, Requests: 4}, {Percent: 50, Requests: 4}, {Percent: 100}}
	must(f.StartRollout(v2, steps, f.Epoch()))
	fmt.Printf("=> v2 got 10%% traffic, min-instances pre-warmed: %+v\n", f.Stats()[v2])
	for i := 0; i < 12; i++ {
		resp, _ := f.Invoke(ctx, &fc.Request{Payload: "canary"})
		st := f.Rollout()
		if st.Active {
			fmt.Printf("=> canary %2d -> %s (rollout step %d/%d, %d%%)\n", i, resp.Revision, st.CurrentStep+1, st.TotalSteps, st.CurrentPercent)
		} else {
			fmt.Printf("=> canary %2d -> %s (rollout finished)\n", i, resp.Revision)
		}
	}
	fmt.Printf("=> rollout done, traffic: %v\n\n", f.Traffic().Weights)

	// 3. 粘性路由：同一路由键始终落到同一版本。
	must(f.UpdateTraffic(map[fc.RevisionID]int{v1: 1, v2: 1}, f.Epoch()))
	for _, key := range []string{"user-a", "user-b", "user-a", "user-b"} {
		resp, _ := f.Invoke(ctx, &fc.Request{Payload: "keyed", RoutingKey: key})
		fmt.Printf("=> key %q -> %s\n", key, resp.Revision)
	}
	fmt.Println("=> same routing key always lands on the same revision")

	// 4. 整体切到 v2，v1 后台排空；v2 的保底实例在空闲后仍常驻。
	must(f.UpdateTraffic(map[fc.RevisionID]int{v2: 1}, f.Epoch()))
	fmt.Println("=> traffic fully shifted to v2; v1 draining in background")
	time.Sleep(500 * time.Millisecond)
	f.Tick()
	fmt.Printf("=> stats after idle: %+v (v2 keeps its min instance, v1 drained)\n\n", f.Stats())

	// 5. 无保底版本缩容到零后冷启动（用 v1 回滚演示）。
	must(f.Rollback(v1, f.Epoch()))
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
