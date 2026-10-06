package fc

import (
	"context"
	"sync"
	"testing"
)

func okHandler(body string) Handler {
	return func(ctx context.Context, req *Request) (string, error) { return body, nil }
}

// TestPublishValidation 非法权重与不存在的版本必须被明确拒绝，且无副作用。
func TestPublishValidation(t *testing.T) {
	f := newTestFunction(t)

	v1, err := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	if v1 == "" {
		t.Fatal("expected non-empty revision id")
	}

	cases := []struct {
		name    string
		weights map[RevisionID]int
		want    ErrorKind
	}{
		{"negative weight", map[RevisionID]int{v1: -1}, ErrKindInvalidWeight},
		{"all zero", map[RevisionID]int{v1: 0}, ErrKindInvalidWeight},
		{"empty", map[RevisionID]int{}, ErrKindInvalidWeight},
		{"unknown revision", map[RevisionID]int{"ghost": 1}, ErrKindUnknownRevision},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := f.Traffic()
			err := f.UpdateTraffic(tc.weights, before.Epoch)
			if !IsKind(err, tc.want) {
				t.Fatalf("want %s, got %v", tc.want, err)
			}
			after := f.Traffic()
			if after.Epoch != before.Epoch || after.Weights[v1] != 1 {
				t.Fatalf("rejected update mutated state: %+v", after)
			}
		})
	}
}

// TestConcurrentPublishConflict 并发发布/回滚只有一个能成功，其余被纪元冲突拒绝。
func TestConcurrentPublishConflict(t *testing.T) {
	f := newTestFunction(t)
	if _, err := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0); err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	epoch := f.Epoch()

	const n = 8
	var wg sync.WaitGroup
	results := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, results[i] = f.Publish(okHandler("vx"), RevisionConfig{}, nil, epoch)
			} else {
				v1 := RevisionID("fn-v000001")
				results[i] = f.Rollback(v1, epoch)
			}
		}(i)
	}
	wg.Wait()

	var succeeded, conflicts int
	for _, err := range results {
		if err == nil {
			succeeded++
		} else if IsKind(err, ErrKindConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	t.Logf("concurrent updates: succeeded=%d conflicts=%d", succeeded, conflicts)
	if succeeded != 1 || conflicts != n-1 {
		t.Fatalf("want exactly 1 success and %d conflicts, got %d/%d", n-1, succeeded, conflicts)
	}
}

// TestRollbackAtomic 回滚整体生效：回滚后全部流量指向目标版本。
func TestRollbackAtomic(t *testing.T) {
	f := newTestFunction(t)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{}, map[RevisionID]int{"": 1}, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	if err := f.Rollback(v1, f.Epoch()); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	tp := f.Traffic()
	t.Logf("after rollback: weights=%v epoch=%d", tp.Weights, tp.Epoch)
	if len(tp.Weights) != 1 || tp.Weights[v1] == 0 {
		t.Fatalf("rollback not atomic: %+v", tp.Weights)
	}

	// 回滚后调用必须全部命中 v1，绝不命中已摘除的 v2。
	for i := 0; i < 20; i++ {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "ping"})
		if err != nil {
			t.Fatalf("invoke: %v", err)
		}
		if resp.Revision != v1 || resp.Body != "v1" {
			t.Fatalf("request routed to %s after rollback, want %s", resp.Revision, v1)
		}
	}
	_ = v2
}

// TestRollbackConflictWithPublish 回滚与并发发布冲突时必须有一方被拒绝。
func TestRollbackConflictWithPublish(t *testing.T) {
	f := newTestFunction(t)
	v1, _ := f.Publish(okHandler("v1"), RevisionConfig{}, map[RevisionID]int{"": 1}, 0)
	epoch := f.Epoch()

	// 先发布 v2（消耗 epoch），再用旧 epoch 回滚：必须冲突。
	if _, err := f.Publish(okHandler("v2"), RevisionConfig{}, nil, epoch); err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if err := f.Rollback(v1, epoch); !IsKind(err, ErrKindConflict) {
		t.Fatalf("stale-epoch rollback must be rejected with conflict, got %v", err)
	}
}
