// Package controlplane 是本地函数计算控制面的门面：
// 编排版本发布、流量分配（CAS）、按版本伸缩的调用与状态查询。
package controlplane

import (
	"context"
	"errors"
	"math/rand"
	"sync"

	"github.com/lisama569/serverless-revision-autoscale-go/internal/autoscale"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/clock"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/fcerr"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/logging"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/revision"
	"github.com/lisama569/serverless-revision-autoscale-go/internal/traffic"
)

// Config 是控制面全局参数。
type Config struct {
	Scale autoscale.Config
}

// InvokeResult 在实例结果上附带请求归因信息。
type InvokeResult struct {
	FunctionName string
	RevisionID   string
	InstanceID   string
	Cold         bool
	Output       []byte
}

type fnState struct {
	revs    *revision.Store
	traffic *traffic.Manager
	scalers map[string]*autoscale.Scaler
}

// ControlPlane 本地函数计算控制面。
type ControlPlane struct {
	cfg Config
	clk clock.Clock
	log logging.Logger
	mu  sync.Mutex
	fns map[string]*fnState
}

// New 创建控制面。
func New(cfg Config, clk clock.Clock, log logging.Logger) *ControlPlane {
	if clk == nil {
		panic("controlplane.New: clock is required")
	}
	if log == nil {
		log = logging.NewJSONLogger(nil)
	}
	cp := &ControlPlane{
		cfg: cfg,
		clk: clk,
		log: log,
		fns: map[string]*fnState{},
	}
	cp.log.Info("controlplane.init")
	return cp
}

// getOrCreateFn 必须在持 cp.mu 时调用。
func (cp *ControlPlane) getOrCreateFn(name string) *fnState {
	st, ok := cp.fns[name]
	if !ok {
		st = &fnState{
			revs:    revision.NewStore(name),
			traffic: traffic.NewManager(),
			scalers: map[string]*autoscale.Scaler{},
		}
		cp.fns[name] = st
	}
	return st
}

func (cp *ControlPlane) getFn(name string) (*fnState, error) {
	st, ok := cp.fns[name]
	if !ok {
		return nil, fcerr.WithMsg(fcerr.ErrFunctionNotFound, "function not found: %s", name)
	}
	return st, nil
}

// Publish 原子地发布一个新版本（仅追加，不自动切流）。
// 新版本拥有不可变、可寻址的 ID（rev-00001…）。
func (cp *ControlPlane) Publish(functionName string, spec revision.Spec) (*revision.Revision, error) {
	cp.mu.Lock()
	st := cp.getOrCreateFn(functionName)
	cp.mu.Unlock()

	rev, err := st.revs.Publish(spec, cp.clk.Now())
	if err != nil {
		return nil, err
	}
	cp.log.Info("controlplane.revision_published",
		"function", functionName, "revision", rev.ID,
		"cold_start", rev.Spec.ColdStart.String(), "max_concurrency", rev.MaxConcurrency())
	return rev, nil
}

// Release 原子地发布新版本并把 100% 流量切到它（典型发布动作）。
// 追加版本与切流在控制面锁内顺序完成；若流量 generation 被并发改动则整体返回 ErrConflict，
// 新版本已发布但流量未切换（调用方可按返回结果重试切流，不存在半生效流量）。
func (cp *ControlPlane) Release(functionName string, spec revision.Spec, expectedGen int64) (*revision.Revision, traffic.Split, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	st := cp.getOrCreateFn(functionName)
	rev, err := st.revs.Publish(spec, cp.clk.Now())
	if err != nil {
		return nil, traffic.Split{}, err
	}
	split, err := st.traffic.Apply(expectedGen,
		map[string]int{rev.ID: traffic.TotalWeight},
		func(revID string) bool {
			_, gErr := st.revs.Get(revID)
			return gErr == nil
		}, cp.clk.Now())
	if err != nil {
		cp.log.Warn("controlplane.release_traffic_conflict",
			"function", functionName, "revision", rev.ID, "err", err.Error())
		return rev, st.traffic.Current(), err
	}
	cp.log.Info("controlplane.release",
		"function", functionName, "revision", rev.ID, "split", split.String())
	return rev, split, nil
}

// GetRevision 查询版本。
func (cp *ControlPlane) GetRevision(functionName, revID string) (*revision.Revision, error) {
	cp.mu.Lock()
	st, err := cp.getFn(functionName)
	cp.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return st.revs.Get(revID)
}

// ListRevisions 列出函数全部版本（按发布顺序）。
func (cp *ControlPlane) ListRevisions(functionName string) ([]*revision.Revision, error) {
	cp.mu.Lock()
	st, err := cp.getFn(functionName)
	cp.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return st.revs.List(), nil
}

// Traffic 返回当前流量分配。
func (cp *ControlPlane) Traffic(functionName string) (traffic.Split, error) {
	cp.mu.Lock()
	st, err := cp.getFn(functionName)
	cp.mu.Unlock()
	if err != nil {
		return traffic.Split{}, err
	}
	return st.traffic.Current(), nil
}

// SetTraffic 以 CAS 方式整体替换权重；expectedGen 传 traffic.GenAny 表示不检查。
// 非法权重、引用不存在版本或 generation 冲突都会被整体拒绝，当前流量保持不变。
func (cp *ControlPlane) SetTraffic(functionName string, expectedGen int64, weights map[string]int) (traffic.Split, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	st, err := cp.getFn(functionName)
	if err != nil {
		return traffic.Split{}, err
	}
	split, err := st.traffic.Apply(expectedGen, weights,
		func(revID string) bool {
			_, gErr := st.revs.Get(revID)
			return gErr == nil
		}, cp.clk.Now())
	if err != nil {
		cp.log.Warn("controlplane.set_traffic_rejected",
			"function", functionName, "expected_gen", expectedGen,
			"weights", weights, "reason", fcerr.ReasonOf(err), "err", err.Error())
		return split, err
	}
	cp.log.Info("controlplane.traffic_set",
		"function", functionName, "split", split.String())
	return split, nil
}

// Rollback 以 CAS 方式把 100% 流量切到指定历史版本。
// 版本不存在返回 ErrRevisionNotFound；generation 冲突返回 ErrConflict。
func (cp *ControlPlane) Rollback(functionName string, expectedGen int64, revID string) (traffic.Split, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	st, err := cp.getFn(functionName)
	if err != nil {
		return traffic.Split{}, err
	}
	if _, gErr := st.revs.Get(revID); gErr != nil {
		return traffic.Split{}, fcerr.WithMsg(fcerr.ErrRevisionNotFound,
			"cannot rollback to missing revision: function=%s revision=%s", functionName, revID)
	}
	split, err := st.traffic.Apply(expectedGen,
		map[string]int{revID: traffic.TotalWeight},
		func(id string) bool {
			_, gErr := st.revs.Get(id)
			return gErr == nil
		}, cp.clk.Now())
	if err != nil {
		cp.log.Warn("controlplane.rollback_conflict",
			"function", functionName, "target", revID, "err", err.Error())
		return split, err
	}
	cp.log.Info("controlplane.rollback",
		"function", functionName, "target", revID, "split", split.String())
	return split, nil
}

// scalerFor 返回（必要时惰性创建）某版本的 Scaler；调用方持 cp.mu。
func (cp *ControlPlane) scalerFor(st *fnState, rev *revision.Revision) *autoscale.Scaler {
	if sc, ok := st.scalers[rev.ID]; ok {
		return sc
	}
	sc := autoscale.New(rev, cp.cfg.Scale, cp.clk,
		cp.log.With("function", rev.FunctionName))
	st.scalers[rev.ID] = sc
	return sc
}

// Invoke 按当前权重选择版本并调用（自动冷启动/排队/背压）。
// rnd 为 nil 时使用全局随机；函数不存在或尚无流量配置时返回对应错误。
//
// 调用结果与错误均带有明确的版本/实例归因（见 InvokeResult 与 fcerr.Reason）。
func (cp *ControlPlane) Invoke(ctx context.Context, functionName string, in []byte, rnd func(n int) int) (*InvokeResult, error) {
	if rnd == nil {
		rnd = rand.Intn
	}
	cp.mu.Lock()
	st, err := cp.getFn(functionName)
	if err != nil {
		cp.mu.Unlock()
		return nil, err
	}
	split := st.traffic.Current()
	if len(split.Weights) == 0 {
		cp.mu.Unlock()
		return nil, fcerr.WithMsg(fcerr.ErrInvalidWeights,
			"no traffic split configured for function=%s", functionName)
	}
	revID, ok := traffic.NewWeightedSelector(split.Weights, rnd).Pick()
	if !ok {
		cp.mu.Unlock()
		return nil, fcerr.WithMsg(fcerr.ErrInvalidWeights,
			"cannot pick revision from empty split for function=%s", functionName)
	}
	rev, gErr := st.revs.Get(revID)
	if gErr != nil {
		// 理论不可达：权重中的版本必然存在。
		cp.mu.Unlock()
		return nil, gErr
	}
	sc := cp.scalerFor(st, rev)
	cp.mu.Unlock()

	log := cp.log.With("function", functionName, "revision", revID)
	res, iErr := sc.Invoke(ctx, in)
	if iErr != nil {
		log.Warn("controlplane.invoke_failed",
			"reason", fcerr.ReasonOf(iErr), "backpressure", fcerr.Backpressure(iErr),
			"err", iErr.Error())
		return nil, iErr
	}
	log.Info("controlplane.invoke_attributed",
		"instance", res.Instance, "cold", res.Cold, "gen", split.Gen)
	return &InvokeResult{
		FunctionName: functionName,
		RevisionID:   res.Revision,
		InstanceID:   res.Instance,
		Cold:         res.Cold,
		Output:       res.Output,
	}, nil
}

// RevisionStats 返回指定版本的伸缩状态。
func (cp *ControlPlane) RevisionStats(functionName, revID string) (autoscale.Stats, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	st, err := cp.getFn(functionName)
	if err != nil {
		return autoscale.Stats{}, err
	}
	if _, gErr := st.revs.Get(revID); gErr != nil {
		return autoscale.Stats{}, gErr
	}
	if sc, ok := st.scalers[revID]; ok {
		return sc.Stats(), nil
	}
	// 从未被调用过的版本：逻辑上处于缩零状态。
	return autoscale.Stats{RevisionID: revID, ScaledToZero: true}, nil
}

// Close 停止并回收所有函数所有版本的实例（幂等）。
func (cp *ControlPlane) Close() error {
	cp.mu.Lock()
	if cp.fns == nil {
		cp.mu.Unlock()
		return nil
	}
	var all []*autoscale.Scaler
	for _, st := range cp.fns {
		for _, sc := range st.scalers {
			all = append(all, sc)
		}
	}
	cp.mu.Unlock()

	var errs []error
	for _, sc := range all {
		if err := sc.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	cp.log.Info("controlplane.closed", "scalers", len(all), "errors", len(errs))
	return errors.Join(errs...)
}

// OnTick 对所有已创建的 Scaler 驱动一次空闲回收检查。
func (cp *ControlPlane) OnTick() {
	cp.mu.Lock()
	var all []*autoscale.Scaler
	for _, st := range cp.fns {
		for _, sc := range st.scalers {
			all = append(all, sc)
		}
	}
	cp.mu.Unlock()
	for _, sc := range all {
		sc.OnTick()
	}
}

// RetireAllInstances 主动回收指定版本的全部实例（用于运维演示/测试）。
// 每个实例先优雅排空；排空超时会强制回收并返回 ErrDrainTimeout。
func (cp *ControlPlane) RetireAllInstances(functionName, revID string) []error {
	cp.mu.Lock()
	st, err := cp.getFn(functionName)
	if err != nil {
		cp.mu.Unlock()
		return []error{err}
	}
	sc, ok := st.scalers[revID]
	cp.mu.Unlock()
	if !ok {
		return nil
	}
	var out []error
	for _, id := range sc.InstanceIDs() {
		if rErr := sc.RetireInstance(id); rErr != nil {
			out = append(out, rErr)
		}
	}
	return out
}
