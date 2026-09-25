package fc

import "errors"

// ErrorKind 对控制面与数据面失败原因进行可区分的分类。
type ErrorKind string

const (
	// 控制面错误
	ErrKindInvalidWeight   ErrorKind = "invalid_weight"             // 权重非法（负数、全零、总和超限等）
	ErrKindUnknownRevision ErrorKind = "unknown_revision"           // 权重指向不存在的版本
	ErrKindConflict        ErrorKind = "concurrent_update_conflict" // 与并发发布/回滚冲突（epoch 不匹配）

	// 数据面背压与生命周期错误
	ErrKindQueueFull      ErrorKind = "backpressure_queue_full" // 排队队列已满，请求被拒绝
	ErrKindDraining       ErrorKind = "revision_draining"       // 版本正在排空，不再接收新流量
	ErrKindDrainTimeout   ErrorKind = "drain_timeout"           // 排空超时，在途请求被取消
	ErrKindInvokeTimeout  ErrorKind = "invoke_timeout"          // 单次调用超时
	ErrKindUpstreamCancel ErrorKind = "upstream_cancel"         // 调用方主动取消
)

// Error 是带分类的错误，调用方可用 KindOf 区分失败原因。
type Error struct {
	Kind ErrorKind
	Msg  string
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Msg }

// KindOf 返回错误的分类；非本包错误返回空串。
func KindOf(err error) ErrorKind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ""
}

// IsKind 判断 err 是否属于指定分类。
func IsKind(err error, k ErrorKind) bool { return KindOf(err) == k }

func newError(k ErrorKind, msg string) *Error { return &Error{Kind: k, Msg: msg} }
