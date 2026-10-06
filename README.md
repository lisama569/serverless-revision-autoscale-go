# serverless-revision-autoscale-go

本地可运行的函数计算控制面：版本化发布、流量权重分配、**渐进放量（灰度）**、
**按业务路由键粘性路由**、按并发负载伸缩（可缩容到零）、**保底常驻实例**、
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

## 渐进放量（灰度）

`StartRollout(plan, expectedEpoch)` 启动一个由请求节奏驱动的放量计划：

- **计划参数**：`Target` 是最终目标权重（与 `UpdateTraffic` 同一套校验）；`Steps` 是
  放量步数（>0）；`RequestsPerStep` 是每两步之间路由的请求数（>0）。
- **节奏语义**：从**当前权重**出发，每累计 `RequestsPerStep` 个已路由请求，整体推进
  到下一步。切换点只取决于请求计数，与时间、并发、随机数无关——同样的请求顺序与步长，
  重复执行得到的每一步切换点和流量分布完全一致。
- **每步原子生效**：每一步本质上都是一次普通权重切换，在同一把锁下整体替换权重表，
  从外部看不存在“切了一半”的中间状态。中间步权重在当前权重与目标权重之间做确定性的
  整数线性插值（公共分母放大 + 最大余数法取整，按版本 ID 排序打破平局）。
- **终态等价**：计划的最后一步原样采用 `Target`。走完后 `Traffic()` 与直接调用一次
  `UpdateTraffic(Target)` 的结果逐键相同，之后的无键请求分布也完全一致。
- **取消 / 打断自洽**：
  - `CancelRollout(expectedEpoch)` 立即停在**当前完整步**的权重上，计划不再推进；
    不再被计划需要、且当前无流量的版本立即进入优雅排空。
  - 放量途中任何普通流量变更（`UpdateTraffic` / 带权重 `Publish` / `Rollback`）都会
    原子地取代计划（计划标记为 `canceled`），新权重当次整体生效，不留半生效/残留状态。
  - 所有操作同样受 epoch 乐观并发保护；重复启动、陈旧纪元返回
    `concurrent_update_conflict`。
- 可通过 `RolloutState()` 观测阶段（`running`/`complete`/`canceled`/`idle`）、
  当前步、步内已计数请求与当前/目标权重。

## 粘性路由（业务路由键）

- 请求在 `Request.RouteKey` 携带业务键（如用户 ID、订单 ID）。键非空时，路由器对键做
  FNV-1a 64 位确定性哈希，映射到当前权重展开区间，因此**在流量策略不变的前提下，
  同一路由键的请求始终落到同一版本**，不依赖随机数；同一请求序列重复运行结果一致。
- **没带路由键**（空串）的请求仍走加权轮转计数器，精确按比例分配（如 1:3 下
  400 个请求精确 100/300）。
- **兼容范围**：粘性是“对当前权重表的纯函数选择”，不缓存键→版本的历史归属。
  因此权重一旦切换，新策略对之后的请求**立即整体生效**；切换前后的策略各自内部，
  同键归属保持稳定。放量计划的每一步切换同样如此。
- 权重为 0 的版本永远不会被粘性请求选中；版本摘除后旧键会确定性地落到剩余版本。

## 保底常驻实例（MinInstances）

`RevisionConfig.MinInstances` 为版本配置保底常驻实例数（默认 0）：

- **无流量也预热**：版本承接流量（或成为进行中放量计划的目标版本，哪怕起步权重为 0）
  时，立即把实例补到 `MinInstances`，灰度首批真实请求不撞冷启动。
- **回收有下限**：空闲缩容时，存活实例数不会低于 `MinInstances`；**保底之外**的空闲
  实例空闲超过 `IdleTimeout` 后照常回收。
- **默认可缩到零**：不配置（0）时行为与以前完全一致——空闲到零、新请求冷启动拉起。
- **下线即收走**：版本彻底失去流量（被普通切换/回滚摘除）、放量计划取消后不再被引用，
  或函数 `Close()` 时，保底实例与其他实例一起进入优雅排空并收走，不会泄漏。
- `MinInstances` 超过 `MaxInstances` 时自动收敛到 `MaxInstances`。

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
go test ./fc/ -v        # 全部测试，日志含每步流量变化、请求归因（版本+实例）、伸缩决策
go test ./fc/ -race     # 竞态检测
go run ./cmd/demo       # 端到端演示：发布→保底预热→渐进放量→粘性路由→切换→排空
```

测试覆盖（日志可看出每一步权重变化、请求最终由哪个版本/实例处理、每次扩容/回收决策）：

- 既有能力不回归：并发发布冲突（`TestConcurrentPublishConflict`）、原子回滚
  （`TestRollbackAtomic`）、权重切换与确定性分布（`TestWeightDistribution`）、
  缩容到零与冷启动（`TestScaleToZeroAndColdStart`）、背压拒绝（`TestBackpressure`）、
  优雅排空与排空超时（`TestGracefulDrain` / `TestDrainTimeout`）、超时与上游取消分类
  （`TestTimeoutClassification`）。
- 渐进放量：逐步推进与每步原子权重（`TestRolloutStepwise`，并断言终态分布与直接切换
  逐请求一致）、节奏可复现（`TestRolloutReproducible`）、中途取消停在自洽比例
  （`TestRolloutCancel`）、被普通切换原子取代（`TestRolloutSuperseded`）、
  参数与纪元校验无副作用（`TestStartRolloutValidation`）。
- 粘性路由：同键稳定 + 无键仍按比例（`TestStickyRoutingStable`）、重复运行结果一致
  （`TestStickyRoutingReproducible`）、权重切换后整体立即生效
  （`TestStickyRoutingSwitchAtomic`）。
- 保底实例：发布即预热、首请求不冷启动（`TestMinInstancesWarmNoColdStart`）、
  保底外实例照常回收（`TestMinInstancesScaleBeyondFloor`）、默认可缩到零
  （`TestDefaultStillScalesToZero`）、下线排空保底实例
  （`TestMinInstancesDrainOnRetire`）、放量期预热与取消后回收
  （`TestMinInstancesWarmDuringRollout`）。
