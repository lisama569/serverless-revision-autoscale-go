package fc

import (
	"sync"
	"testing"
)

// safeLogger 包装 testing.T.Logf，使其可被后台排空 goroutine 与测试主体并发调用。
// 配合 newTestFunction 的 t.Cleanup(Close)，所有后台 goroutine 都会在用例结束前汇合，
// 不会跨用例调用 testing 日志。
type safeLogger struct {
	mu sync.Mutex
	tb testing.TB
}

func (s *safeLogger) logf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tb.Helper()
	s.tb.Logf(format, args...)
}

// newTestFunction 创建函数并在用例结束时自动 Close（汇合全部后台排空 goroutine）。
func newTestFunction(t *testing.T) *Function {
	t.Helper()
	sl := &safeLogger{tb: t}
	f := NewFunction("fn", sl.logf)
	t.Cleanup(f.Close)
	return f
}
