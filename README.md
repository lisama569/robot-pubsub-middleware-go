# robot-pubsub-middleware-go

进程内机器人组件通信中间件（Go）。支持主题通配发布/订阅、可校验序列号与版本、
可靠投递与断线续传、**可选背压策略**、**严格的确认位点校验**与**按去向分开的
消息统计**，以及并发安全的生命周期管理。全部验证在本机完成，无需任何外部服务。

## 快速开始

```go
b := middleware.NewBroker(middleware.Config{
    SupportedVersions: []middleware.Version{middleware.Version1},
    QueueSize:         64,   // 每个订阅的缓冲队列长度
    Retention:         1024, // 全局保留日志条数（用于续传重放）
})
defer b.Close()

// 建立订阅时声明队列满后的处理策略。
sub, _ := b.SubscribeWith("robot/#", 0, middleware.SubscribeOptions{
    Policy: middleware.PolicyDropOldest, // 或 PolicyDropNewest
})
seq, _ := b.Publish("robot/telemetry", middleware.Version1, []byte("payload"))
_ = seq

for m := range sub.C() { // 通道排空后自动结束（range）
    sub.Ack(m.Seq) // 推进确认位点
}
```

> `Subscribe(pattern, resumeFrom)` 仍保留，等价于用默认策略
> `PolicyDropOldest` 调 `SubscribeWith`，旧代码无需改动。

## 主题匹配规则

主题以 `/` 分段，订阅模式支持两种通配符（MQTT 风格）：

| 模式 | 语义 | 示例 |
| --- | --- | --- |
| 字面段 | 逐段相等 | `a/b/c` 仅匹配 `a/b/c` |
| `*` | 匹配恰好一段 | `a/*/c` 匹配 `a/b/c`，不匹配 `a/b` 或 `a/b/c/d` |
| `#` | 仅可作最后一段，匹配零段或多段 | `a/#` 匹配 `a`、`a/b`、`a/b/c` |

空主题或空模式一律不匹配；发布空主题返回 `ErrEmptyTopic`。

## 消息、序列号与版本

- 每条成功发布的消息由 Broker 分配**全局单调递增**的 `Seq`（从 1 开始）。
- 校验失败（空主题 / 版本不兼容 / 已关闭）的发布**不占用序列号**，
  因此订阅侧看到的序列号严格连续递增，可直接作为确认位点。
- 版本不在 `SupportedVersions` 内的消息在发布入口被**明确拒绝**，
  返回 `ErrUnsupportedVersion`，不会静默丢弃，更不会进入任何订阅队列。
- 同一订阅内，序列号不大于已交付水位的重复消息被拒绝交付，
  并计入 `Stats.Duplicates`。

## 背压策略（建立订阅时声明）

每个订阅有独立的定长缓冲队列（`QueueSize`）。发布路径对订阅队列只做
**非阻塞**操作：无论哪种策略，**慢订阅者都不会阻塞发布方或其他订阅者**。
策略通过 `SubscribeOptions.Policy` 声明，生命周期内不变：

| 策略 | 队列满时的行为 | 谁被丢 | 丢失计数 |
| --- | --- | --- | --- |
| `PolicyDropOldest`（默认） | **留着最新的**：挤掉一条最旧消息给新消息腾位，新消息照常入队 | 最旧的未消费消息 | `DroppedOldest` |
| `PolicyDropNewest` | **留着先来的**：直接拒收这条新消息，已排队的消息原封不动 | 新来的消息 | `DroppedNewest` |

- 未知策略值在 `SubscribeWith` 处被明确拒绝，返回 `ErrInvalidPolicy`；
  零值 `PolicyDefault` 规范化为 `PolicyDropOldest`。
- 两种丢法**分别计数、互不混淆**，可据 `Subscription.Policy()` 与
  统计字段判断到底是哪一步、以哪种方式丢的。

## 确认位点：严格校验与可区分原因

`Ack(seq)` 表示确认 `seq`（含）之前的消息。除了原有的“不许回退”，
现在还会拒绝“确认到没交到手的位置”和“跨过未补齐的空档”。空档进一步分成
**还来得及补齐**与**已超出保留范围、无法补齐**两种，拒绝原因互不相同，
都可用 `errors.Is` 区分，且**被任何一种原因拒绝时确认位点都保持不变**：

| 错误 | 触发条件 | 建议处置 |
| --- | --- | --- |
| `ErrPositionRegression` | `seq` 小于当前已确认位点（位点回退） | 修正调用方逻辑 |
| `ErrAckNotDelivered` | `seq` 超过该订阅实际已交到手上的水位（确认了一个从没交付的位置；续传暂停期间确认尚未补到的位点也属此类） | 等消息真正交到手上再确认 |
| `ErrAckGap` | `(当前位点, seq]` 区间内存在**见过但已判定丢失、尚未补齐、且仍在保留窗口内**的位点（例如 DropOldest 已挤掉、还没靠重连续传补回） | **继续等补齐**：保持位点，尽快从当前确认位点重连即可补回 |
| `ErrAckGapExpired` | 区间内的空档位点**已滚出保留窗口**，怎么重连都无法从保留日志补回（它同时 wrap `ErrAckGap`，见下） | **这个位点已经没救，尽快重置订阅**（从头订阅或按业务重置位点），而不是继续等待 |

**边界判定（两种“只差一点点”不会互相误判）**：设保留窗口最早仍存活的
消息为 `oldest`，区间内最早的丢失位点为 `g`：

- `g >= oldest`（包括 `g == oldest`，刚好卡在窗口边缘）：判定为
  **可补齐**的 `ErrAckGap`——这条消息还在窗口里，重连一定能补到；
- `g < oldest`（哪怕只差 1，即 `g == oldest-1`）：判定为
  **不可补齐**的 `ErrAckGapExpired`。

这与 `SubscribeWith` 的续传合法性边界完全一致：`resumeFrom == oldest-1`
仍允许建立订阅（`oldest` 起的消息都能补），更早的位点返回
`ErrPositionExpired`。Broker 每发布一条消息、保留窗口发生裁剪时，都会
把新的 `oldest` 水位推送给所有在线订阅，因此并发发布期间空档“刚刚过期”
的瞬间，确认也能立刻得到正确分类，不会停留在旧的可补齐结论上。

**错误分类的包含关系**：`ErrAckGapExpired` 同时 wrap `ErrAckGap`，即

```go
errors.Is(err, ErrAckGap)         // “任意空档”：可补齐和不可补齐都命中
errors.Is(err, ErrAckGapExpired)  // 仅彻底不可补齐的空档命中
```

只想区分“有空档 vs. 其它非法确认”时用前者；要决定“继续等还是赶紧重置”
时必须再用后者细分。只检查区间内**最早的一个**丢失位点即可决定原因，
错误信息会同时给出该位点、当前确认位点和 `oldest`，便于对账。

非匹配主题的序列号不计入空档——只订阅 `a` 时，确认位点可以越过只发到 `b`
的消息（缺口按“匹配该订阅的消息”计算，而非全局每一条）。

## 投递、续传与兼容范围

- `SubscribeWith(pattern, resumeFrom, opts)`：`resumeFrom` 为已确认位点
  （全局序列号），`0` 表示从头开始。
- 建立订阅时，Broker 从保留日志中**按全局顺序、在队列有空位时懒重放**
  `Seq > resumeFrom` 且匹配模式的消息——包括该订阅上一代因背压丢过、
  但仍在保留窗口内的缺口消息。续传期间队列满只会**暂停补入**（不会再次
  淘汰或拒收），消费腾出空位后继续，因此**补完后的结果与“一直在线且始终
  跟得上”一模一样，不重不漏**。续传进行中到达的实时消息会排在缺口之后，
  保证严格按序。
- 重放受 `Retention` 限制，建立订阅时会校验位点并给出可区分错误：
  - `ErrPositionExpired`：`resumeFrom` 早于保留窗口的最早可重放消息，
    中间缺口已无法从日志补齐，明确拒绝而不是静默少给；
  - `ErrAckNotDelivered`：`resumeFrom` 超过 Broker 已发布的最大位点；
  - `resumeFrom == 0`（从头开始）不受保留窗口限制，只给窗口内尚存的消息。
- 在线消费期间遇到空档，按 `Ack` 的错误分类处置：收到可补齐的 `ErrAckGap`
  时保持当前位点、稍后（或立即）从该位点重连即可，补完后的视图与一直在线
  且跟得上的订阅一模一样；收到 `ErrAckGapExpired` 说明空档已永久丢失，
  再怎么重连也补不回，应按业务重置（如从头订阅）而不是空等。
- 订阅退订（`Unsubscribe`）或 Broker 关闭后，已入队消息仍可排空读取，
  排空后交付通道关闭（`range` 正常结束）。

## 消息去向统计（退订/关闭后可查、可对账）

`Subscription.Stats()` 返回本次订阅生命周期内的消息去向汇总，
各项语义互不重叠，退订或关闭后仍可查询且定格不变：

| 字段 | 语义 |
| --- | --- |
| `Delivered` | 成功进入订阅队列的匹配消息数（含之后被挤掉的） |
| `DroppedOldest` | DropOldest 下、队列满时被挤掉的最旧消息数 |
| `DroppedNewest` | DropNewest 下、队列满时被拒收的新消息数 |
| `DroppedLate` | 退订或 Broker 关闭**之后**才到达、因而被丢的匹配消息数 |
| `Duplicates` | 序列号不大于已交付水位、因重复被挡下的消息数 |

勾稽关系（单个订阅，重复消息不属于“新匹配消息”，单列）：

```
匹配消息总数 = Delivered + DroppedNewest + DroppedLate
实际读到     = Delivered - DroppedOldest        # 被挤掉的旧消息确实入过队
```

## 并发与关闭语义

- `Publish` / `SubscribeWith` / `Unsubscribe` / `Close` 可任意并发调用：
  锁序固定为 `broker.mu -> sub.mu`，不会死锁；`Close` 与 `Unsubscribe` 幂等。
- 对交付通道的写入/淘汰都在订阅锁内以非阻塞方式原子完成，消费者无锁读通道，
  不存在“先看队列长度再收发”的竞态。
- `Close` 先停止后台续传泵、再在锁内关闭各订阅通道，不会向已关闭通道投递；
  关闭后 `Publish` / `SubscribeWith` 返回 `ErrClosed`，关闭前已入队消息可排空。

## 本地验证

无需任何外部服务，直接运行：

```sh
go test ./...              # 全部单元测试
go test -race ./...        # 竞态检测（可通过）
go test -v ./middleware/   # 查看序列号、位点与判定依据日志

# 只跑确认空档三类判定（可补齐 / 已过期 / 边界）与收敛、并发压力用例：
go test -v -run 'TestAckGap|TestConcurrentGapExpiry' ./middleware/
```

测试覆盖：

| 测试 | 场景 |
| --- | --- |
| `TestMatchTopic` | 主题与通配模式匹配规则 |
| `TestPublishVersionValidation` | 未知版本明确拒绝、被拒发布不占序列号 |
| `TestBackpressureDropOldest` | 留新挤旧：慢订阅不阻塞他人、挤旧计数 |
| `TestBackpressureDropNewest` | 留旧拒新：先到的保留、新消息拒收、不阻塞 |
| `TestBackpressureCountersSeparated` | 两种丢法分别计数、互不串 |
| `TestSubscribePolicyValidation` | 默认策略归一化、未知策略 `ErrInvalidPolicy` |
| `TestAckPositionRegression` / `TestAckInvalidReasons` | 回退 / 未交付 / 空档三类非法确认可区分 |
| `TestAckGapFilledByResume` / `TestAckDuringPartialFill` | 缺口续传补齐，中途与补完后的确认语义 |
| `TestAckGapStillRecoverable` | 丢失位点仍在保留窗口：`ErrAckGap`（可补齐）拒绝，位点不动 |
| `TestAckGapExpiredBeyondRetention` | 丢失位点滚出窗口：`ErrAckGapExpired` 拒绝且仍归类 `ErrAckGap`，重连也补不回 |
| `TestAckGapRetentionBoundary` | 卡在窗口边缘的判定：`g == oldest` 可补齐、`g == oldest-1` 即过期，与续传边界一致 |
| `TestAckGapRejectThenResumeConverges` | 拒绝后继续消费并多代重连补齐，最终视图与一直在线且跟得上的订阅一致（不重不漏） |
| `TestConcurrentGapExpiryTinyQueues` | 多发布方×多订阅者、队列容量 2、保留窗口 32：空档边产生边过期，不卡死、位点不漂移、账目对得上 |
| `TestAckIgnoresNonMatchingSeq` | 非匹配主题序列号不构成空档 |
| `TestResumePositionValidation` | 续传位点过期 `ErrPositionExpired` 与超前 `ErrAckNotDelivered` |
| `TestResumeAfterReconnect` / `TestConcurrentResume` | 断线续传不丢不重、多代重连位点一致 |
| `TestStatsReconciliation` 等 | 去向四项分开计数、勾稽关系、关闭后定格可查 |
| `TestCloseSemantics` | 关闭幂等、关闭前排空、关闭后 `ErrClosed` |
| `TestConcurrentLifecycle` | 并发发布/订阅/退订/关闭无死锁无 panic |
| `TestConcurrentUnsubscribeDuringReplay` | 续传积压期间并发退订不杀死续传泵 |
| `TestConcurrentMixedBackpressure` | 多发布方×多订阅方、两种策略、极小队列：不卡死且账目对得上 |
| `TestConcurrentTinyQueueResume` | 队列容量 1 的高并发多代续传，最终位点不重不漏 |
