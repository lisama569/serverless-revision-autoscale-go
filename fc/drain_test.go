package fc

import (
	"context"
	"testing"
	"time"
)

// TestGracefulDrain 排空中的实例不再接收新流量，但在途请求必须完成；
// 同函数其他版本流量不受影响。
func TestGracefulDrain(t *testing.T) {
	f := newTestFunction(t)
	release := make(chan struct{})
	v1Handler := func(ctx context.Context, req *Request) (string, error) {
		<-release
		return "v1-done", nil
	}
	v1, _ := f.Publish(v1Handler, RevisionConfig{DrainTimeout: 2 * time.Second},
		map[RevisionID]int{"": 1}, 0)
	v2, err := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// v1 上一个在途请求。
	inflight := make(chan struct{})
	go func() {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "inflight"})
		if err != nil {
			t.Errorf("in-flight request must complete, got %v", err)
		} else if resp.Body != "v1-done" {
			t.Errorf("unexpected body %q", resp.Body)
		}
		close(inflight)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for f.Stats()[v1].InFlight != 1 {
		if time.Now().After(deadline) {
			t.Fatal("in-flight request did not start")
		}
		time.Sleep(time.Millisecond)
	}

	// 流量整体切到 v2：v1 开始排空。
	if err := f.UpdateTraffic(map[RevisionID]int{v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("switch traffic: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for f.Stats()[v1].Draining != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("v1 instance not draining: %+v", f.Stats()[v1])
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("v1 draining: %+v, v2: %+v", f.Stats()[v1], f.Stats()[v2])

	// 排空期间新请求全部命中 v2。
	for i := 0; i < 10; i++ {
		resp, err := f.Invoke(context.Background(), &Request{Payload: "new"})
		if err != nil {
			t.Fatalf("v2 invoke during drain: %v", err)
		}
		if resp.Revision != v2 {
			t.Fatalf("draining revision received new traffic: %s", resp.Revision)
		}
	}

	// 在途请求完成后，v1 实例被回收。
	close(release)
	<-inflight
	deadline = time.Now().Add(2 * time.Second)
	for f.Stats()[v1].Instances != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("v1 not drained to zero: %+v", f.Stats()[v1])
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("v1 drained to zero; v2 unaffected: %+v", f.Stats()[v2])
}

// TestDrainTimeout 排空超时与调用超时、上游取消可区分。
func TestDrainTimeout(t *testing.T) {
	f := newTestFunction(t)
	block := func(ctx context.Context, req *Request) (string, error) {
		<-ctx.Done() // 永不主动完成，等取消
		return "", ctx.Err()
	}
	v1, _ := f.Publish(block, RevisionConfig{
		DrainTimeout:  50 * time.Millisecond,
		InvokeTimeout: 10 * time.Second, // 调用超时远大于排空超时
	}, map[RevisionID]int{"": 1}, 0)
	v2, _ := f.Publish(okHandler("v2"), RevisionConfig{}, nil, f.Epoch())

	errCh := make(chan error, 1)
	go func() {
		_, err := f.Invoke(context.Background(), &Request{Payload: "stuck"})
		errCh <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for f.Stats()[v1].InFlight != 1 {
		if time.Now().After(deadline) {
			t.Fatal("in-flight request did not start")
		}
		time.Sleep(time.Millisecond)
	}

	if err := f.UpdateTraffic(map[RevisionID]int{v2: 1}, f.Epoch()); err != nil {
		t.Fatalf("switch traffic: %v", err)
	}
	select {
	case err := <-errCh:
		if !IsKind(err, ErrKindDrainTimeout) {
			t.Fatalf("want drain_timeout, got %v", err)
		}
		t.Logf("in-flight request terminated with: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("drain timeout did not terminate in-flight request")
	}
}

// TestTimeoutClassification 调用超时与上游取消可区分。
func TestTimeoutClassification(t *testing.T) {
	f := newTestFunction(t)
	block := func(ctx context.Context, req *Request) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if _, err := f.Publish(block, RevisionConfig{InvokeTimeout: 50 * time.Millisecond},
		map[RevisionID]int{"": 1}, 0); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// 调用超时。
	_, err := f.Invoke(context.Background(), &Request{Payload: "slow"})
	if !IsKind(err, ErrKindInvokeTimeout) {
		t.Fatalf("want invoke_timeout, got %v", err)
	}
	t.Logf("slow invoke: %v", err)

	// 上游取消。
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	_, err = f.Invoke(ctx, &Request{Payload: "cancelled"})
	if !IsKind(err, ErrKindUpstreamCancel) {
		t.Fatalf("want upstream_cancel, got %v", err)
	}
	t.Logf("cancelled invoke: %v", err)
}
