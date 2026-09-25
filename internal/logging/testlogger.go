package logging

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// TB 是 testing.T 与 testing.B 的公共接口。
type TB interface {
	Logf(format string, args ...any)
}

type testLogger struct {
	t   TB
	tag string
}

// NewTestLogger 返回将结构化日志写入 t.Logf 的 logger。
func NewTestLogger(t testing.TB) Logger {
	return &testLogger{t: t}
}

func callerShort() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return "???"
	}
	if i := strings.LastIndex(file, "/"); i >= 0 {
		file = file[i+1:]
	}
	return fmt.Sprintf("%s:%d", file, line)
}

func (l *testLogger) render(level, msg string, args []any) string {
	kv := make([]string, 0, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		kv = append(kv, fmt.Sprintf("%s=%v", args[i], args[i+1]))
	}
	if len(args)%2 == 1 {
		kv = append(kv, fmt.Sprintf("?extra=%v", args[len(args)-1]))
	}
	tag := ""
	if l.tag != "" {
		tag = l.tag + " "
	}
	return fmt.Sprintf("[%s] %s%s %s | %s", strings.ToUpper(level), tag, callerShort(), msg, strings.Join(kv, " "))
}

func (l *testLogger) Info(msg string, args ...any)  { l.t.Logf("%s", l.render("info", msg, args)) }
func (l *testLogger) Warn(msg string, args ...any)  { l.t.Logf("%s", l.render("warn", msg, args)) }
func (l *testLogger) Error(msg string, args ...any) { l.t.Logf("%s", l.render("error", msg, args)) }
func (l *testLogger) Debug(msg string, args ...any) { l.t.Logf("%s", l.render("debug", msg, args)) }

func (l *testLogger) With(args ...any) Logger {
	parts := make([]string, 0, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		parts = append(parts, fmt.Sprintf("%s=%v", args[i], args[i+1]))
	}
	nl := &testLogger{t: l.t, tag: strings.TrimSpace(l.tag + " " + strings.Join(parts, " "))}
	return nl
}
