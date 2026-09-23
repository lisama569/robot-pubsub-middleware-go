# robot-pubsub-middleware-go

进程内机器人组件通信中间件（Go）。支持主题通配发布/订阅、可校验序列号与版本、
可靠投递与断线续传、背压控制以及并发安全的生命周期管理。全部验证在本机完成，
无需任何外部服务。

## 快速开始

```go
b := middleware.NewBroker(middleware.Config{
    SupportedVersions: []middleware.Version{middleware.Version1},
    QueueSize:         64,   // 每个订阅的缓冲队列长度
    Retention:         1024, // 全局保留日志条数（用于续传重放）
})
defer b.Close()

sub, _ := b.Subscribe("robot/#", 0) // 0 表示从头开始；重连时传入上次确认位点
seq, _ := b.Publish("robot/telemetry", middleware.Version1, []byte("payload"))

m := <-sub.C()
sub.Ack(m.Seq) // 推进确认位点
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

## 投递与续传语义

- `Subscribe(pattern, resumeFrom)`：`resumeFrom` 为已确认位点（全局序列号）。
  订阅建立时，Broker 从保留日志中重放 `Seq > resumeFrom` 且匹配模式的消息，
  之后实时消息接续投递。重连时传入上次 `Position()` 即可从确认位点继续，
  已确认消息不重复、未确认消息不丢失。
- 重放受 `Retention` 限制：早于保留窗口的消息不可恢复（确定语义）。
- `Ack(seq)` 推进确认位点；**位点回退被明确拒绝**，返回包装
  `ErrPositionRegression` 的错误，其中携带当前位点与尝试值，便于区分原因。
- 订阅退订（`Unsubscribe`）或 Broker 关闭后，已入队消息仍可排空读取，
  排空后交付通道关闭（`range` 正常结束）。

## 背压处理策略

- 每个订阅有独立的定长缓冲队列（`QueueSize`），发布路径对订阅队列只做
  非阻塞写入：**慢订阅者不会阻塞发布方或其他订阅者**。
- 队列满时按确定策略**淘汰最旧消息**（drop-oldest），新消息仍然入队。
- 统计可区分（`Subscription.Stats()`）：
  - `Delivered`：成功入队的消息数；
  - `DroppedFull`：因队列超限、在被消费前被淘汰的最旧消息数；
  - `DroppedLate`：关闭后到达被丢弃的消息数；
  - `Duplicates`：因序列号重复被拒绝交付的消息数。

## 并发与关闭语义

- `Publish` / `Subscribe` / `Unsubscribe` / `Close` 可任意并发调用：
  锁序固定为 `broker.mu -> sub.mu`，不会死锁；`Close` 与 `Unsubscribe` 幂等。
- `Close` 之后：`Publish` / `Subscribe` 返回 `ErrClosed`；
  关闭前已入队各订阅缓冲的消息保留并可排空，排空后通道关闭；
  关闭后到达订阅的消息计入 `DroppedLate`，不再交付。

## 本地验证

无需任何外部服务，直接运行：

```sh
go test ./...          # 全部单元测试
go test -race ./...    # 竞态检测（可通过）
go test ./middleware/ -v   # 查看序列号、位点与判定依据日志
```

测试覆盖：

| 测试 | 场景 |
| --- | --- |
| `TestMatchTopic` | 主题与通配模式匹配规则 |
| `TestPublishVersionValidation` | 未知版本明确拒绝、被拒发布不占序列号 |
| `TestAckPositionRegression` | 位点回退拒绝并给出可区分原因 |
| `TestResumeAfterReconnect` | 断线续传：从确认位点继续，不丢不重 |
| `TestNoDuplicateDeliveryOnResubscribe` | 重复消息不重复交付 |
| `TestBackpressureDropOldest` | 慢订阅者不阻塞他人、超限淘汰最旧、统计可区分 |
| `TestCloseSemantics` | 关闭幂等、关闭前排空、关闭后 `ErrClosed` |
| `TestConcurrentLifecycle` | 并发发布/订阅/退订/关闭无死锁无 panic |
| `TestConcurrentResume` | 多代重连续传，最终位点与发布总数一致 |
