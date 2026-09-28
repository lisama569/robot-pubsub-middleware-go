# robot-pubsub-middleware-go

进程内机器人组件通信中间件（Go）。支持主题通配发布/订阅、可校验序列号与版本、
可靠投递与断线续传（缺口自动补齐）、**按订阅可选的背压策略**、**严格确认位点校验**
以及并发安全的生命周期管理。全部验证在本机完成，无需任何外部服务。

## 快速开始

```go
b := middleware.NewBroker(middleware.Config{
    SupportedVersions: []middleware.Version{middleware.Version1},
    QueueSize:         64,   // 每个订阅的缓冲队列长度
    Retention:         1024, // 全局保留日志条数（用于续传重放）
})
defer b.Close()

// 建订阅时声明队列满后的处理办法（零值 = drop-oldest，兼容旧行为）。
sub, _ := b.SubscribeWith("robot/#", 0, middleware.SubscribeOptions{
    OverflowPolicy: middleware.PolicyDropNewest, // 留先来的，新的先不收
})
seq, _ := b.Publish("robot/telemetry", middleware.Version1, []byte("payload"))

m := <-sub.C()
sub.Ack(m.Seq) // 推进确认位点（见下方“确认位点规则”）
```

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
- 同一订阅内，序列号不大于已入队位点的重复消息被拒绝交付，
  并计入 `Stats.Duplicates`。

## 背压处理策略

每个订阅有独立的定长缓冲队列（`QueueSize`），发布路径对订阅队列只做
**非阻塞** channel 操作：**慢订阅者不会阻塞发布方或其他订阅者**。
建订阅时通过 `SubscribeOptions.OverflowPolicy` 按订阅声明队列满后的办法：

| 策略 | 行为 | 丢弃计数 |
| --- | --- | --- |
| `PolicyDropOldest`（默认，零值） | **留最新、挤最旧**：新消息照常入队，尚未消费的最旧消息被挤出 | `DroppedOldest` |
| `PolicyDropNewest` | **留先来的、新的先不收**：队列里的消息不动，新消息直接拒收；队列腾出空位后自动恢复投递 | `DroppedNewest` |

两类丢弃分别计数，不会混成一个总数，方便判断到底卡在哪一步。

## 确认位点规则（严格校验）

`Ack(seq)` 表示确认 `seq`（含）之前的消息已全部、连续交付。除既有的“回退”外，
以下情形都会被**明确拒绝**，且原因可通过 `errors.Is` 区分，错误信息携带位点细节：

| 错误 | 触发情形 |
| --- | --- |
| `ErrPositionRegression` | `seq` 小于当前位点（回退） |
| `ErrAckNotDelivered` | `seq` 超过本订阅已交付的前沿位点（还没交到手上的位置不能确认） |
| `ErrAckGap` | 确认区间内存在未交付**空档**（消息曾命中本订阅，但因队列满被丢），中间留了没确认的洞 |
| `ErrPositionExpired` | 空档对应的消息（或续传位点）已**滚出保留窗口**，不可恢复 |

被拒绝的确认不会推进位点。处理方式：拿当前 `Position()` 作为 `resumeFrom` 重新订阅，
由 Broker 从保留日志把缺口补齐（见下节），补完即可连续确认。

## 投递、缺口补齐与续传

- `Subscribe(pattern, resumeFrom)` / `SubscribeWith(...)`：`resumeFrom` 为已确认
  位点（全局序列号），`0` 表示从头开始。订阅建立后 Broker 先按序重放保留日志中
  `Seq > resumeFrom` 且匹配模式的消息，再无缝接续重放期间到达的实时消息。
- **缺口补齐结果与一直在线完全一致**：重放严格按序、不重不漏；队列暂满时按
  消费者排空节奏等待重投，不会在补齐过程中再次丢消息（要求消费者最终能排空队列；
  补齐结束后若发生持续过载，仍按该订阅声明的策略背压）。
- 重放受 `Retention` 限制：`resumeFrom` 早于保留窗口起点时返回包装
  `ErrPositionExpired` 的错误，订阅不会建立（明确拒绝，绝不静默丢段）。
- 退订（`Unsubscribe`）或 Broker 关闭后，已入队消息仍可排空读取，排空后通道关闭。

## 订阅去向统计（可对账）

`Subscription.Stats()` 在退订/关闭后仍可查询，返回该订阅本次生命周期的去向汇总：

| 字段 | 语义 |
| --- | --- |
| `Matched` | 命中本订阅、进入投递判定的消息总数 |
| `Delivered` | 成功进入队列的消息数（含后来被挤掉、或关闭后未排空的） |
| `DroppedOldest` | drop-oldest 下入队后、消费前被挤出的条数（**Delivered 的子项**） |
| `DroppedNewest` | drop-newest 下被直接拒收、从未入队的条数 |
| `DroppedLate` | 关闭后到达被丢弃的条数（重放/竞态路径，从未入队） |
| `Duplicates` | 因重复被挡下的条数 |

恒等式（代码中可用 `Stats.Reconciled()` 校验）：

```
Matched == Delivered + DroppedNewest + DroppedLate + Duplicates
Delivered >= DroppedOldest   // DroppedOldest 是 Delivered 的子项
```

注意 `DroppedOldest` 不再与右侧各项并列相加：被挤掉的消息是“先入队、后淘汰”，
份额已包含在 `Delivered` 内。

## 兼容性

- 默认策略 `PolicyDropOldest`（零值）与上一版“淘汰最旧”行为一致；
  旧的 `Subscribe(pattern, resumeFrom)` 签名与用法完全保留。
- 统计字段 `DroppedFull` 更名为 `DroppedOldest`（语义不变），并新增
  `Matched` / `DroppedNewest`；旧代码引用需改名。
- 协议版本当前仅 `Version1`，由 `Config.SupportedVersions` 控制兼容范围。

## 并发与关闭语义

- `Publish` / `Subscribe` / `SubscribeWith` / `Unsubscribe` / `Ack` / `Close`
  可任意并发调用：投递路径全程使用非阻塞 channel 操作，锁序固定为
  `broker.mu -> sub.mu`，不会死锁；`Close` 与 `Unsubscribe` 幂等。
- `Close` 会等待进行中的建连重放收尾，因此关闭后查到的 `Stats` 恒等式完整；
  之后的 `Publish` / `Subscribe` 返回 `ErrClosed`，关闭前已入队消息可排空。

## 本地验证

无需任何外部服务，直接运行：

```sh
go test ./...                  # 全部单元测试
go test -race ./...            # 竞态检测（可通过）
go test -count=30 ./middleware/   # 反复执行，验证并发稳定性
go test ./middleware/ -v       # 查看序列号、位点与判定依据日志
```

测试覆盖：

| 测试 | 场景 |
| --- | --- |
| `TestMatchTopic` | 主题与通配模式匹配规则 |
| `TestPublishVersionValidation` | 未知版本明确拒绝、被拒发布不占序列号 |
| `TestAckPositionRegression` | 位点回退拒绝并给出可区分原因 |
| `TestAckValidationDistinctions` | 四类非法确认（回退/未交付/空档/过期）互不混淆 |
| `TestAckGapWithDropNewest` / `TestAckGapExpiredByRetention` | 空档确认拒绝、过期空档单独区分 |
| `TestSubscribeResumeExpired` | 续传位点早于保留窗口被明确拒绝 |
| `TestResumeFillsDropOldestGap` / `TestResumeFillsDropNewestGap` | 两类策略造成的空档重连续传补齐 |
| `TestResumeEquivalentToAlwaysOnline` | 补齐结果与一直在线逐 seq 一致，补齐后可连续确认 |
| `TestResumeCatchUpTinyQueueUnderLoad` | 极小队列 + 持续负载下边补边收不卡死、不重不漏 |
| `TestResumeAfterReconnect` / `TestConcurrentResume` | 断线续传与多代续传不丢不重 |
| `TestNoDuplicateDeliveryOnResubscribe` | 重复消息不重复交付 |
| `TestBackpressureDropOldest` / `TestBackpressureDropNewest` | 两种策略各自行为与分策略计数 |
| `TestBackpressurePoliciesIsolated` | 两种策略订阅并存互不干扰、发布方不被拖住 |
| `TestBackpressureDropNewestRecovers` | 队列腾出空位后拒收自动恢复 |
| `TestStatsReconciliation` / `TestStatsAfterBrokerClose` / `TestStatsEmpty` | 去向汇总恒等式与关闭后可查询 |
| `TestCloseSemantics` | 关闭幂等、关闭前排空、关闭后 `ErrClosed` |
| `TestConcurrentLifecycle` | 并发发布/订阅/退订/关闭无死锁无 panic |
| `TestConcurrentBackpressureStress` | 多发布方 + 多订阅者（混合策略）极小队列 + 中途重连：不卡死、序列递增、统计对得上 |
