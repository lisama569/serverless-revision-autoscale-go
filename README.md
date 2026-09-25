# serverless-revision-autoscale-go

本地可运行的函数计算控制面：版本化发布、流量权重分配、按并发负载伸缩（可缩容到零）、
实例优雅排空与可区分的背压/超时错误分类。不依赖任何外部服务或凭据。

## 版本与流量语义

- **不可变可寻址版本**：每次 `Publish` 产生一个不可变 `Revision`（ID 形如 `hello-v000001`，
  单调递增），包含 handler 与 `RevisionConfig` 快照，发布后即可寻址、可回滚。
- **原子流量切换**：`UpdateTraffic` / 带权重的 `Publish` / `Rollback` 都在单把锁下
  校验并整体替换权重表，不存在只生效一半的中间状态。
- **乐观并发控制**：所有变更携带 `expectedEpoch`，与当前纪元不匹配（如与并发发布、
  回滚冲突）即返回 `concurrent_update_conflict`，状态不变。并发 N 个变更至多 1 个成功。
- **权重校验**：空权重表、负权重、全零权重、指向不存在版本的权重均被明确拒绝
  （`invalid_weight` / `unknown_revision`），且无任何副作用。
- **发布时切换流量**：`Publish` 的权重表中，键 `""` 指代本次发布的新版本。
- **流量恢复**：版本失去流量后池被排空；若之后（如回滚）重新获得流量，会为其创建新池，
  旧池继续在后台排空，互不影响。

## 伸缩与排空规则

- **按需扩容**：请求到达时，先在现有实例上占并发槽；无可用槽且实例数未达
  `MaxInstances` 时冷启动新实例。
- **缩容到零**：`Tick()` 回收空闲超过 `IdleTimeout` 的实例；无流量时实例数收敛到 0，
  新请求到来时重新拉起（冷启动，实例 ID 更新）。
- **优雅排空**：实例进入 draining 后不再接收新流量；在途请求继续执行直至完成，
  或直到 `DrainTimeout` 被取消。排空不波及同函数其他版本的流量。
- **背压**：所有实例饱和时请求进入容量为 `QueueSize` 的有界队列；队列满立即以
  `backpressure_queue_full` 拒绝——不静默丢弃、不无限堆积。排队中上游取消按
  `upstream_cancel` 返回。

## 错误分类（`fc.KindOf(err)`）

| Kind | 含义 |
|---|---|
| `invalid_weight` | 权重非法（空表/负数/全零） |
| `unknown_revision` | 权重或回滚目标指向不存在的版本 |
| `concurrent_update_conflict` | 纪元不匹配，与并发发布/回滚冲突 |
| `no_traffic` | 函数当前无流量配置 |
| `backpressure_queue_full` | 实例饱和且排队队列已满 |
| `revision_draining` | 版本/实例正在排空，不接收新流量 |
| `drain_timeout` | 排空超时，在途请求被取消 |
| `invoke_timeout` | 单次调用超过 `InvokeTimeout` |
| `upstream_cancel` | 调用方取消（含排队中取消） |

## 本地验证

```sh
go test ./fc/ -v        # 全部测试，日志含请求归因、伸缩决策与状态变化
go test ./fc/ -race     # 竞态检测
go run ./cmd/demo       # 端到端演示：发布→灰度→切换→缩容到零→冷启动→排空
```

测试覆盖：并发发布冲突（`TestConcurrentPublishConflict`）、权重切换与确定性流量分布
（`TestWeightDistribution`，1:3 权重 400 次调用精确 100/300）、缩容到零与冷启动
（`TestScaleToZeroAndColdStart`，注入时钟使判定可重复）、背压拒绝
（`TestBackpressure`）、优雅排空与排空超时（`TestGracefulDrain` / `TestDrainTimeout`）、
调用超时与上游取消的分类（`TestTimeoutClassification`）。
