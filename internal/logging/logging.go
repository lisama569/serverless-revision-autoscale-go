// Package logging 提供控制面统一的结构化日志接口。
//
// 控制面内部统一通过 Logger 打印「请求归因、伸缩决策、关键状态变化」，
// 测试通过 NewTestLogger 将日志输出到 t.Log，保证失败时可在测试日志中追溯。
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// Logger 是结构化日志的最小接口。
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
	With(args ...any) Logger
}

type slogLogger struct{ l *slog.Logger }

func (s *slogLogger) Info(msg string, args ...any)  { s.l.Info(msg, args...) }
func (s *slogLogger) Warn(msg string, args ...any)  { s.l.Warn(msg, args...) }
func (s *slogLogger) Error(msg string, args ...any) { s.l.Error(msg, args...) }
func (s *slogLogger) Debug(msg string, args ...any) { s.l.Debug(msg, args...) }
func (s *slogLogger) With(args ...any) Logger       { return &slogLogger{l: s.l.With(args...)} }

// NewJSONLogger 将 JSON 行式日志写入 w（默认 os.Stderr）。
func NewJSONLogger(w io.Writer) Logger {
	if w == nil {
		w = os.Stderr
	}
	return &slogLogger{l: slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))}
}

// NewTextLogger 将人类可读日志写入 w（默认 os.Stderr）。
func NewTextLogger(w io.Writer) Logger {
	if w == nil {
		w = os.Stderr
	}
	return &slogLogger{l: slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))}
}

type ctxKey struct{}

// WithLogger 将 logger 放入 context。
func WithLogger(ctx context.Context, l Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext 取出 context 中的 logger；不存在返回 nil。
func FromContext(ctx context.Context) Logger {
	l, _ := ctx.Value(ctxKey{}).(Logger)
	return l
}

// FromOrDefault 取出 context 中的 logger；不存在返回 def。
func FromOrDefault(ctx context.Context, def Logger) Logger {
	if l := FromContext(ctx); l != nil {
		return l
	}
	return def
}
