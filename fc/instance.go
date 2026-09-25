package fc

// instanceState 实例生命周期状态。
type instanceState int

const (
	stateActive instanceState = iota
	stateDraining
	stateDead
)

// instance 是一个函数实例，按 MaxConcurrency 限制并发。
type instance struct {
	id    string
	state instanceState
}
