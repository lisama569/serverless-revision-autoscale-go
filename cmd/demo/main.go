// demo 在本地演示：版本发布、渐进放量、粘性路由、保底实例、缩容到零与优雅排空。
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

	// 1. 发布 v1（保底 1 实例，空闲 300ms 以下不回收）并承接全部流量。
	v1, err := f.Publish(func(ctx context.Context, req *fc.Request) (string, error) {
		return "hello from v1", nil
	}, fc.RevisionConfig{IdleTimeout: 300 * time.Millisecond, MinInstances: 1},
		map[fc.RevisionID]int{"": 1}, f.Epoch())
	must(err)
	fmt.Printf("=> v1 published with MinInstances=1, stats: %+v\n", f.Stats()[v1])

	resp, _ := f.Invoke(ctx, &fc.Request{Payload: "ping", RouteKey: "user-42"})
	fmt.Printf("=> sticky user-42 -> %s on %s\n\n", resp.Revision, resp.InstanceID)

	// 2. 发布 v2（保底 2 实例）：灰度期间即使还没分到流量，也已预热好。
	v2, err := f.Publish(func(ctx context.Context, req *fc.Request) (string, error) {
		return "hello from v2", nil
	}, fc.RevisionConfig{MinInstances: 2}, nil, f.Epoch())
	must(err)

	// 3. 渐进放量：4 步从 v1-only 走到 v1:v2 = 1:1，每 5 个请求推进一步。
	must(f.StartRollout(fc.RolloutPlan{
		Target:          map[fc.RevisionID]int{v1: 1, v2: 1},
		Steps:           4,
		RequestsPerStep: 5,
	}, f.Epoch()))
	fmt.Println("=> rollout started (4 steps, 5 requests/step); v2 pre-warmed:")
	for i := 0; i < 20; i++ {
		r, _ := f.Invoke(ctx, &fc.Request{Payload: "rollout"})
		if i%5 == 4 {
			st := f.RolloutState()
			fmt.Printf("=> after %2d requests: step=%d/%d weights=%v (last -> %s)\n",
				i+1, st.Step, st.Steps, st.CurrentWeights, r.Revision)
		}
	}

	// 4. 粘性路由：同一业务键在当前策略下永远落到同一版本。
	pins := map[string]fc.RevisionID{}
	for _, k := range []string{"user-1", "user-2", "user-3", "user-4"} {
		pins[k] = invokeKey(f, ctx, k)
	}
	for round := 0; round < 3; round++ {
		for _, k := range []string{"user-1", "user-2", "user-3", "user-4"} {
			if got := invokeKey(f, ctx, k); got != pins[k] {
				log.Fatalf("sticky key %s moved: %s -> %s", k, pins[k], got)
			}
		}
	}
	fmt.Printf("=> sticky routing stable across rounds: %v\n\n", pins)

	// 5. 普通权重切换立即整体生效；无保底的额外版本仍可缩容到零。
	must(f.UpdateTraffic(map[fc.RevisionID]int{v2: 1}, f.Epoch()))
	fmt.Println("=> traffic fully shifted to v2; v1 draining (its min instances are reclaimed)")
	time.Sleep(300 * time.Millisecond)
	f.Tick()
	fmt.Printf("=> stats after switch: v1=%+v v2=%+v\n", f.Stats()[v1], f.Stats()[v2])

	f.Close()
	fmt.Println("=> all instances drained, demo done")
}

func invokeKey(f *fc.Function, ctx context.Context, key string) fc.RevisionID {
	r, err := f.Invoke(ctx, &fc.Request{Payload: "sticky", RouteKey: key})
	if err != nil {
		log.Fatal(err)
	}
	return r.Revision
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
