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

## 渐进放量（灰度发布）

- **请求节奏的多步放量**：`StartRollout(newRev, steps, expectedEpoch)` 按
  `[]RolloutStep{{Percent, Requests}, ...}` 计划放量。第一步随调用立即原子生效；
  之后每经过当前步 `Requests` 次调用（每次 `Invoke` 计一次），推进一步。
- **每步整体生效**：每一步的权重表都在控制面锁内整体替换，外部观测到的永远是
  某一步的完整权重，不存在只切了一半的中间状态（`TestRolloutAtomicSteps` 并发观测验证）。
- **精确比例**：第 `p%` 步时新版本精确占 `p%`，其余版本按启动时的基准比例瓜分
  剩余份额；权重表按最大公约数归一化，计划走完后的状态与直接做一次等比例
  `UpdateTraffic` 的结果**完全一致**（`TestProgressiveRollout` 与参照函数逐键对比）。
- **取消与回退自洽**：`CancelRollout()` 让流量停在上一个已整体生效的步比例上，
  无半生效残留；放量中途 `Rollback` / `UpdateTraffic` / 带权重的 `Publish`
  会整体替换权重并自动终结放量计划，同样不留残留状态。
- **可复现**：推进只由调用次数驱动，不依赖墙钟与随机数；同样的请求顺序和步长，
  重复跑出的切换点与流量分布逐次一致。
- **校验**：空计划、百分比越界 `(0,100]`、不严格递增、非末步 `Requests<=0`、
  目标版本不存在、已有放量进行中（`rollout_in_progress`）均被拒绝且无副作用。

## 粘性路由

- **按业务路由键粘滞**：`Request.RoutingKey` 非空时，对键做 FNV-1a 哈希，
  在按版本 ID 排序展开的权重序列上定位版本。同一键在流量策略不变时**始终**
  落到同一版本；权重切换后按新表重新映射并再次保持稳定。
- **兼容范围**：不带键的请求仍按权重做确定性轮转，且带键请求不消耗轮转计数器，
  两类流量互不干扰；粘性是版本级的，不保证落到同一实例。
- **可复现**：不依赖随机数，同一请求序列重复跑的归因完全一致
  （`TestStickyRouting` 重跑比对）。

## 保底常驻实例

- **`RevisionConfig.MinInstances`**：版本持有流量期间，即使空闲也保留这么多实例
  （获得流量时即预热），灰度试跑的第一个真实请求不会撞上冷启动。
- **与缩容到零的关系**：保底数之外的实例空闲超过 `IdleTimeout` 照常回收；
  默认 `MinInstances=0`，行为与之前完全一致（可缩到零、新请求冷启动拉起）。
- **排空不泄漏**：版本失去流量进入排空时，保底实例随整池一并回收；
  排空后 `Tick` 不会为无流量版本重新预热。

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
| `invalid_weight` | 权重非法（空表/负数/全零）；放量计划非法（空计划/百分比越界/不递增/非正请求数） |
| `unknown_revision` | 权重、回滚或放量目标指向不存在的版本 |
| `concurrent_update_conflict` | 纪元不匹配，与并发发布/回滚冲突 |
| `rollout_in_progress` | 已有渐进放量在进行中，拒绝再启动第二个 |
| `no_traffic` | 函数当前无流量配置 |
| `backpressure_queue_full` | 实例饱和且排队队列已满 |
| `revision_draining` | 版本/实例正在排空，不接收新流量 |
| `drain_timeout` | 排空超时，在途请求被取消 |
| `invoke_timeout` | 单次调用超过 `InvokeTimeout` |
| `upstream_cancel` | 调用方取消（含排队中取消） |

## 本地验证

```sh
go test ./fc/ -v        # 全部测试，日志含每步流量变化、请求归因、伸缩与回收决策
go test ./fc/ -race     # 竞态检测
go run ./cmd/demo       # 端到端演示：发布→渐进放量→粘性路由→保底常驻→切换→排空→冷启动
```

测试覆盖：并发发布冲突（`TestConcurrentPublishConflict`）、权重切换与确定性流量分布
（`TestWeightDistribution`，1:3 权重 400 次调用精确 100/300）、渐进放量的步进节奏/
原子性/可复现性与直接切换等价（`TestProgressiveRollout` / `TestRolloutAtomicSteps`）、
放量取消与中途回退的自洽性（`TestRolloutCancel` / `TestRolloutRollbackMidway`）、
放量计划校验（`TestRolloutValidation`）、粘性路由稳定性与可复现性
（`TestStickyRouting` / `TestStickyAfterWeightChange` / `TestStickyDoesNotDisturbRoundRobin`）、
保底常驻实例的预热/回收/排空（`TestMinInstancesKeepWarm` /
`TestMinInstancesDefaultUnchanged` / `TestMinInstancesDrainedOnTrafficLoss`）、
缩容到零与冷启动（`TestScaleToZeroAndColdStart`，注入时钟使判定可重复）、背压拒绝
（`TestBackpressure`）、优雅排空与排空超时（`TestGracefulDrain` / `TestDrainTimeout`）、
调用超时与上游取消的分类（`TestTimeoutClassification`）。
