# 本地函数计算控制面（Go）

一个**完全本地、零外部依赖、零凭据**的函数计算控制面参考实现，覆盖：

- 函数的**版本化发布**（不可变、可寻址 revision）与**流量权重分配**（原子整体生效、CAS 乐观锁）；
- **按并发负载伸缩**与**无流量缩容到零**，新请求自动重新冷启动；
- 实例回收时的**优雅排空（drain）**，在途请求完成或得到明确失败；
- 可区分的**背压与错误分类**，不静默丢弃、不无限堆积；
- 丰富的结构化日志与可重复的并发测试（含 `-race`）。

所有时间抽象在 [`internal/clock`](internal/clock) 之后，测试用 `FakeClock` 确定性驱动
冷启动、调用超时、空闲缩零与排空超时，**验证不依赖任何外部服务或凭据**。

---

## 1. 架构与包结构

| 包 | 职责 |
| --- | --- |
| `internal/fcerr` | 错误类型与机器可判别的 `Reason`、`errors.Is` 哨兵、背压判定 |
| `internal/clock` | `Clock` 接口、`RealClock` 与可控的 `FakeClock`（`Advance` 同步触发定时器） |
| `internal/logging` | 结构化日志接口（JSON/Text）与写入 `t.Log` 的 `TestLogger` |
| `internal/revision` | 不可变版本（revision）的追加存储，ID 形如 `rev-00001` |
| `internal/traffic` | 权重分配快照 `Split`、校验、generation CAS、加权选择器 |
| `internal/instance` | 单实例状态机（Starting/Active/Draining/Stopped）、冷启动、带超时调用、优雅排空 |
| `internal/autoscale` | 单版本实例池：扩缩容、有界准入队列、背压、空闲缩零、回收排空 |
| `internal/controlplane` | 门面：发布 / Release / SetTraffic / Rollback / Invoke / 状态查询 / Close |
| `cmd/fc-demo` | 本地可运行的端到端演示 |

调用链路：

```
ControlPlane.Invoke
  └─ 读取当前 traffic.Split（不可变快照）
      └─ WeightedSelector.Pick()  → 选定 revision
          └─ 该 revision 的 Scaler.Invoke
              ├─ 准入计数（执行容量 + 队列容量）满 → 立即 ErrQueueFull
              ├─ 入有界队列
              └─ serveOne：取执行令牌 → 选热实例 / 冷启动新实例 → Instance.Invoke
```

---

## 2. 版本与流量语义

### 2.1 版本（revision）

- `Publish(name, spec)` 原子追加一个新版本，分配全局可寻址 ID `rev-00001、rev-00002 …`。
- 版本**不可变**：ID、序号、创建时间与 `Spec`（Handler、冷启动耗时、单实例并发、元数据）发布后不再修改；
  发布时会拷贝元数据，调用方后续改动不影响已发布版本。
- 新版本**只追加、不自动切流**；`Release(name, spec, expectedGen)` 是「发布 + 把 100% 流量切到新版本」的组合动作。

### 2.2 流量权重（traffic split）

- 当前流量是一份不可变快照 `Split{Gen, Weights, UpdatedAt}`。
- 权重为百分比，**合计必须恰好 100**；每个权重必须为正；**每个目标版本必须存在**。
- `SetTraffic(name, expectedGen, weights)` 以**整份权重**为单位更新：
  - 先完整校验（含版本存在性），通过后在锁内一次性替换；
  - **非法权重 / 幽灵版本 → 整体拒绝，当前流量完全不变，不存在半生效中间状态**；
  - `expectedGen` 与当前代次不符 → `ErrConflict`，调用方可重读 `Traffic()` 后重试（CAS）；
  - 传 `traffic.GenAny`（`-1`）表示不检查代次；每次成功切换 `Gen` 单调 +1。
- `Rollback(name, expectedGen, revID)` 把 100% 流量切回任意**已存在的历史版本**，
  与并发发布/切流通过同一把 generation CAS 互斥：过期者得到 `ErrConflict`。
- 选择器对版本 ID 排序后按整数区间落点，可注入 `rnd` 做**确定性、可重复**的分布验证。

> 说明：`Release` 中「追加版本」与「切流」是控制面锁内的两步。若切流因 generation 冲突失败，
> 新版本**仍已发布且可寻址**，但流量保持原样（不会把一半流量导向未完成的发布）；调用方据返回的
> `ErrConflict` 重试切流即可。这保证了流量视图的原子性与可解释性。

---

## 3. 伸缩与排空规则

### 3.1 扩容（scale out）

- 每个 revision 有独立 `Scaler`，**版本之间实例池完全隔离**。
- 请求先过**准入计数** `admitted`（CAS），上限 = `MaxScale × 单实例MaxConcurrency + QueueCapacity`：
  - 未超限：进入有界队列，由调度循环派发给 `serveOne`；
  - 已超限：**立即**返回背压错误，请求不排队、不阻塞。
- `serveOne` 先取**执行令牌**（容量 = 总执行并发），再选择实例：
  - 存在 `FreeSlots()>0` 的 Active 实例 → 直接复用；
  - 否则若未达 `MaxScale` → 同步**冷启动**一个新实例（计入 `starting`，受令牌约束避免过量创建）；
  - 否则在队列中等待槽位释放（实例完成调用 / 回收会唤醒等待者）。
- 冷启动完成前取消/超时，实例会被剔除并 `ForceStop`，错误归类为取消或冷启动超时。

### 3.2 缩容到零（scale to zero）

- `OnTick()`（建议每 `IdleTick` 调用一次）检查实例：`在途==0` 且空闲时长 ≥ `IdleTimeout` 的 Active 实例被回收。
- 最后一个实例被回收后，该 revision `Stats.ScaledToZero == true`。
- 缩零后下一个请求会重新走冷启动路径，`Result.Cold == true`。

### 3.3 优雅排空（drain）

- `BeginDrain()` 将实例置为 `Draining`（幂等）：**不再接收新流量**（新请求被明确拒绝，原因 `drain_timeout`），
  但**已接收的在途请求继续执行**直到完成、调用超时或上游取消。
- `Drain(ctx, timeout)` 等待在途请求（`sync.WaitGroup`）：
  - 全部完成 → 实例 `Stopped`，返回 `nil`；
  - 超过 `timeout` → 返回 `ErrDrainTimeout`，实例**保持 Draining**，可再次 `Drain` 或 `ForceStop`；
  - 调用方 `ctx` 取消 → `ErrUpstreamCanceled`。
- `ForceStop()` 立即回收并广播 `stopCh`，所有在途请求得到明确的 `ErrDrainTimeout`。
- **排空只影响被回收的那个版本/实例，不波及同一函数其他版本的流量**（scaler 相互独立）。
- `Close()` 会拒绝队列中尚未处理的请求、排空（超时则强制）全部 Active 与 Starting 实例，
  并等待冷启动 goroutine 退出后才返回（幂等）。

---

## 4. 背压与错误分类

所有错误在 [`internal/fcerr`](internal/fcerr/fcerr.go) 中带机器可判别的 `Reason`，
可用 `errors.Is(err, fcerr.ErrXxx)` 或 `fcerr.ReasonOf(err)` 区分；`fcerr.Backpressure(err)`
判定是否为可安全重试的背压。

| Reason / 哨兵 | 触发时机 | 是否可重试背压 |
| --- | --- | --- |
| `invalid_weights` | 权重为空/非正/合计非 100；函数尚无流量配置就调用 | 否（请求错误） |
| `revision_not_found` | 权重或回滚指向不存在的版本；查询不存在版本 | 否 |
| `conflict` | 流量/回滚 CAS 的 generation 过期 | 是（重读后代次重试） |
| `function_not_found` | 查询一个从未创建过的函数 | 否 |
| `queue_full` | 已达「执行容量 + 队列容量」，请求被立即拒绝 | **是** |
| `cold_start_timeout` | 等待冷启动超过请求截止时间 | **是** |
| `invoke_timeout` | 在途执行超过实例 `InvokeTimeout` 或请求 deadline | 视调用方策略 |
| `upstream_canceled` | 上游主动 `cancel()`（排队中或在途中） | 否（调用方放弃） |
| `drain_timeout` | 排空期间新流量被拒 / 在途请求被强制回收打断 / 排空超时 | 视场景 |
| `invocation_failed` | 函数 Handler 自身返回错误 | 视业务 |

关键不变量：

- **不静默丢弃**：超容量一律以 `queue_full` 显式拒绝；`Close` 清空队列时每个请求都收到明确结果。
- **不无限堆积**：准入上限有界，执行令牌把「冷启动等待 + 在途执行」限制在总并发容量内。
- **原因可区分**：排队/冷启动/执行超时、上游取消、排空超时、业务失败各自独立归因。
- **请求归因可解释**：成功结果带 `RevisionID / InstanceID / Cold`；日志记录权重代次、选中实例与伸缩决策。

---

## 5. 本地验证（无需任何外部服务或凭据）

前置：Go 1.25+。

```bash
# 1) 端到端演示：发布、非法权重拒绝、90/10 分布、CAS 冲突、全切/回滚、
#    背压、优雅排空超时、缩零与冷启动重拉
go run ./cmd/fc-demo

# 2) 全量单元/集成测试
go test ./...

# 3) 竞态检测（建议）
go test -race ./...

# 4) 查看结构化日志中的请求归因 / 伸缩决策 / 状态变化
go test -v ./internal/controlplane
go test -v ./internal/autoscale
```

测试覆盖（均为确定性或可重复断言）：

- `internal/traffic`：权重校验各非法形态、CAS 成功/冲突、并发竞争恰好一个成功、加权分布精确、空选择器。
- `internal/instance`：冷启动与冷热归因、冷启动超时 vs 上游取消、调用超时 vs 取消、
  Handler 失败、排空中拒新但在途完成、排空超时保持 Draining、ForceStop 打断在途、并发槽位。
- `internal/autoscale`：从零冷启动与热实例复用、`queue_full` 背压计数、空闲缩零与冷启动重拉、
  优雅回收在途成功、排空超时强制回收（在途得 `drain_timeout`）、排队中取消可区分、冷启动超时背压。
- `internal/controlplane`：版本不可变/可寻址、流量校验与 CAS、**16 路并发 Release 恰一胜出**、
  80/15/5 确定性分布、回滚（幽灵版本/冲突/成功）、缩零后切流冷启动、**回收一个版本不影响另一版本**、
  无流量调用被拒。

测试通过 `logging.NewTestLogger(t)` 将「请求归因、伸缩决策（scale_out/reap_idle_instance/
instance_retired）、关键状态变化（cold_start_begin/done、drain_begin/complete、reject_queue_full）」
写入测试日志，`go test -v` 即可完整追溯。

---

## 6. 快速上手示例

```go
cp := controlplane.New(controlplane.Config{Scale: autoscale.Config{
    MaxScale: 4, QueueCapacity: 32,
    IdleTimeout: 2 * time.Minute, DrainTimeout: 10 * time.Second, InvokeTimeout: 5 * time.Second,
}}, clock.RealClock{}, logging.NewJSONLogger(os.Stderr))
defer cp.Close()

// 首次发布（自动 100% 流量）
v1, split, _ := cp.Release("hello", revision.Spec{Handler: h1}, traffic.GenAny)

// 灰度：读取代次 -> 整体设置权重
split, _ = cp.SetTraffic("hello", split.Gen, map[string]int{v1.ID: 90 /*, v2: 10 */})

// 调用（结果带版本/实例/冷热归因）
res, err := cp.Invoke(ctx, "hello", []byte("ping"), nil)

// 冲突重试（CAS）
if errors.Is(err, fcerr.ErrConflict) { cur, _ := cp.Traffic("hello"); /* 用 cur.Gen 重试 */ }
```
