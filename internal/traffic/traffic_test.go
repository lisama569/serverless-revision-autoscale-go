package traffic

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
)

func exists(ids ...string) func(string) bool {
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	return func(id string) bool { return set[id] }
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		weights map[string]int
		ex      func(string) bool
		want    fcerr.Reason
	}{
		{"empty", map[string]int{}, exists("a"), fcerr.ReasonInvalidWeights},
		{"zero weight", map[string]int{"a": 0, "b": 100}, exists("a", "b"), fcerr.ReasonInvalidWeights},
		{"negative", map[string]int{"a": -1, "b": 101}, exists("a", "b"), fcerr.ReasonInvalidWeights},
		{"wrong sum", map[string]int{"a": 90, "b": 9}, exists("a", "b"), fcerr.ReasonInvalidWeights},
		{"ghost revision", map[string]int{"a": 50, "x": 50}, exists("a"), fcerr.ReasonRevisionNotFound},
		{"valid", map[string]int{"a": 90, "b": 10}, exists("a", "b"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.weights, tc.ex)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if fcerr.ReasonOf(err) != tc.want {
				t.Fatalf("want reason=%s got=%v", tc.want, err)
			}
		})
	}
}

func TestApplyAtomicCAS(t *testing.T) {
	m := NewManager()
	now := time.Unix(1, 0)

	// 初始非法更新必须完全不生效。
	if _, err := m.Apply(GenAny, map[string]int{"a": 90}, exists("a"), now); err == nil {
		t.Fatal("expected invalid weights rejection")
	}
	if got := m.Current(); got.Gen != 0 || len(got.Weights) != 0 {
		t.Fatalf("state mutated after rejected apply: %+v", got)
	}

	// 首次成功更新：gen 0->1。
	s1, err := m.Apply(GenAny, map[string]int{"a": 100}, exists("a"), now)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if s1.Gen != 1 {
		t.Fatalf("want gen=1 got=%d", s1.Gen)
	}

	// 过期 generation 必须冲突拒绝，且状态不变。
	_, err = m.Apply(0, map[string]int{"b": 100}, exists("b"), now.Add(time.Second))
	if !errors.Is(err, fcerr.ErrConflict) {
		t.Fatalf("want conflict got=%v", err)
	}
	if got := m.Current(); got.Gen != 1 || got.Weights["a"] != 100 {
		t.Fatalf("state mutated after conflict: %+v", got)
	}

	// 当前 generation 成功更新。
	s2, err := m.Apply(1, map[string]int{"a": 70, "b": 30}, exists("a", "b"), now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("apply current gen: %v", err)
	}
	if s2.Gen != 2 || s2.Weights["b"] != 30 {
		t.Fatalf("unexpected split: %+v", s2)
	}
}

// TestConcurrentApplyOnlyOneSucceeds：N 个并发发布者基于同一 gen 竞争，
// 必须恰好一个成功，其余全部 ErrConflict，最终代次只前进 1。
func TestConcurrentApplyOnlyOneSucceeds(t *testing.T) {
	m := NewManager()
	base, err := m.Apply(GenAny, map[string]int{"a": 100}, exists("a"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	const n = 32
	var wg sync.WaitGroup
	var okMu sync.Mutex
	ok, conflicts := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := map[string]int{"a": 100}
			_, err := m.Apply(base.Gen, w, exists("a"), time.Now())
			okMu.Lock()
			defer okMu.Unlock()
			if err == nil {
				ok++
			} else if errors.Is(err, fcerr.ErrConflict) {
				conflicts++
			}
		}(i)
	}
	wg.Wait()
	if ok != 1 || conflicts != n-1 {
		t.Fatalf("want exactly 1 success and %d conflicts, got ok=%d conflicts=%d", n-1, ok, conflicts)
	}
	if got := m.Current(); got.Gen != base.Gen+1 {
		t.Fatalf("want final gen=%d got=%d", base.Gen+1, got.Gen)
	}
}

func TestWeightedSelectorDistribution(t *testing.T) {
	weights := map[string]int{"rev1": 70, "rev2": 25, "rev3": 5}
	// 确定性 rnd：依次返回 0..99，等价均匀扫描全部落点。
	i := 0
	sel := NewWeightedSelector(weights, func(n int) int { x := i % n; i++; return x })
	counts := map[string]int{}
	for k := 0; k < 100; k++ {
		id, ok := sel.Pick()
		if !ok {
			t.Fatal("pick returned false")
		}
		counts[id]++
	}
	want := map[string]int{"rev1": 70, "rev2": 25, "rev3": 5}
	if fmt.Sprint(counts) != fmt.Sprint(want) {
		t.Fatalf("distribution=%v want=%v", counts, want)
	}
}

func TestWeightedSelectorEmpty(t *testing.T) {
	sel := NewWeightedSelector(map[string]int{}, nil)
	if _, ok := sel.Pick(); ok {
		t.Fatal("empty selector must return ok=false")
	}
}
