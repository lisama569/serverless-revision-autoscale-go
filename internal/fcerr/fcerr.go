// Package fcerr 定义控制面的错误类型与可区分的失败原因。
//
// 分类总览：
//   - 参数/状态类：ErrInvalidWeights、ErrRevisionNotFound、ErrConflict、ErrFunctionNotFound
//   - 背压类：ErrQueueFull、ErrColdStartTimeout（retryable）
//   - 调用生命周期类：ErrInvokeTimeout、ErrUpstreamCanceled、ErrDrainTimeout、ErrInvocationFailed
package fcerr

import (
	"errors"
	"fmt"
)

// Reason 是机器可判别的失败原因。
type Reason string

// 预定义失败原因。
const (
	ReasonInvalidWeights   Reason = "invalid_weights"
	ReasonRevisionNotFound Reason = "revision_not_found"
	ReasonConflict         Reason = "conflict"
	ReasonFunctionNotFound Reason = "function_not_found"

	ReasonQueueFull        Reason = "queue_full"
	ReasonColdStartTimeout Reason = "cold_start_timeout"

	ReasonInvokeTimeout    Reason = "invoke_timeout"
	ReasonUpstreamCanceled Reason = "upstream_canceled"
	ReasonDrainTimeout     Reason = "drain_timeout"
	ReasonInvocationFailed Reason = "invocation_failed"
)

// Error 携带可判别的 Reason 与可读消息。
type Error struct {
	Reason Reason
	Msg    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Msg)
}

func newError(r Reason, format string, args ...any) *Error {
	return &Error{Reason: r, Msg: fmt.Sprintf(format, args...)}
}

// 哨兵错误，可用 errors.Is 判别。
var (
	ErrInvalidWeights   = newError(ReasonInvalidWeights, "")
	ErrRevisionNotFound = newError(ReasonRevisionNotFound, "")
	ErrConflict         = newError(ReasonConflict, "")
	ErrFunctionNotFound = newError(ReasonFunctionNotFound, "")

	ErrQueueFull        = newError(ReasonQueueFull, "")
	ErrColdStartTimeout = newError(ReasonColdStartTimeout, "")

	ErrInvokeTimeout    = newError(ReasonInvokeTimeout, "")
	ErrUpstreamCanceled = newError(ReasonUpstreamCanceled, "")
	ErrDrainTimeout     = newError(ReasonDrainTimeout, "")
	ErrInvocationFailed = newError(ReasonInvocationFailed, "")
)

// WithMsg 基于哨兵错误构造带具体上下文消息的错误（保留 Reason）。
func WithMsg(target *Error, format string, args ...any) error {
	return &Error{Reason: target.Reason, Msg: fmt.Sprintf(format, args...)}
}

// Is 支持 errors.Is(err, ErrXxx)：同 Reason 即视为同类。
func (e *Error) Is(target error) bool {
	var t *Error
	if errors.As(target, &t) {
		return t.Reason == e.Reason
	}
	return false
}

// ReasonOf 返回错误对应的 Reason；无法识别返回空串。
func ReasonOf(err error) Reason {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

// Backpressure 判断是否为背压类错误（调用方可安全重试）。
func Backpressure(err error) bool {
	switch ReasonOf(err) {
	case ReasonQueueFull, ReasonColdStartTimeout:
		return true
	default:
		return false
	}
}
