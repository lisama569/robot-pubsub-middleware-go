# robot-pubsub-middleware-go

一个**纯进程内、零外部依赖**的机器人组件通信中间件。支持主题/通配发布订阅、
带序列号与版本的消息校验、基于位点 ACK 的可靠投递与断线续传、确定的背压策略，
以及发布/订退/关闭全并发下不死锁、不 panic、结果可解释的生命周期管理。

仅依赖 Go 标准库，全部验证在本机完成，无需任何外部服务。

- Go：1.25+（标准库即可）
- Module：`github.com/lisama569/robot-pubsub-middleware-go`

---

## 1. 主题匹配规则

主题与模式都以 `.` 分层，例如 `robot.arm.joint1.temp`。

| 符号 | 位置 | 含义 |
| --- | --- | --- |
| `.` | 任意 | 层级分隔符 |
| `*` | 模式任意层 | 匹配**恰好一个**层级 |
| `#` | 只能位于模式末尾 | 匹配**零到多个**层级 |

规则与判定依据：

- 具体主题（发布侧）不得包含 `*`、`#`，不得为空、不得含空层级（前导/末尾/连续点），
  否则返回 `invalid_topic`。
- 模式中的 `#` 只能作为最后一段，例如 `robot.#` 合法，`a.#.b` 非法。
- 逐段匹配：普通段要求逐字相等；`*` 消费恰好一个主题段；`#` 吞掉所有剩余层（含零层）。

匹配示例：

| 模式 | 主题 | 命中 |
| --- | --- | --- |
| `robot.*.temp` | `robot.arm.temp` | ✅ |
| `robot.*.temp` | `robot.arm.joint.temp` | ❌（`*` 仅一层） |
| `robot.#` | `robot` | ✅（`#` 匹配零层） |
| `robot.#` | `robot.arm.joint.temp` | ✅ |
| `#` | `anything.at.all` | ✅ |

---

## 2. 消息模型：序列号与版本

每条消息（`Message`）携带：

- `Seq uint64`：**生产者序列号**，同一 `Producer` 内从 1 开始严格单调递增；
- `Version{Major, Minor}`：线路格式版本，当前 `v1.0`，最低支持 `v1.0`；
- `Producer`：生产者标识；`Topic`；`Payload`。

发布到 Broker 时的校验（全部为**明确拒绝**，返回 `*RejectError`，绝不静默丢弃、不 panic）：

| 情况 | 拒绝原因 RejectReason |
| --- | --- |
| 序列号为 0 | `invalid_sequence` |
| 同一 Producer 序列号重复或回退（`seq <= lastSeq`） | `invalid_sequence` |
| 版本为零值（未设置） | `unknown_version` |
| 版本主版本高于当前（如 `v2.0`，本进程不认识） | `unknown_version` |
| 版本主版本低于最低支持（如 `v0.x`） | `incompatible_version` |
| 同主版本不同 Minor（如 `v1.5`） | **接受**（同主版本向后兼容） |

> 序列号允许"跳号"（gap），跳号不拒绝（传感器偶尔丢采样不等于协议错误）；
> 只有**重复与回退**被拒绝。Broker 日志会打印 seq 与判定依据。

此外每条被接受的消息由 Broker 分配一个**全局单调位点** `Position`（从 1 开始），
它与生产者 `Seq` 相互独立：`Seq` 用于业务/生产侧校验，`Position` 用于投递确认与续传。

---

## 3. 投递、ACK 与断线续传语义

### 3.1 位点与确认

- 订阅者从 `Deliveries()` 通道读取 `Delivery{Position, Message}`。
- `Ack(position)` 确认所有 `Position <= position` 的消息，位点**只能单调前进**。
- 确认结果被严格校验，原因可区分：

| ACK 情况 | 结果 | RejectReason |
| --- | --- | --- |
| `pos < committed`（回退） | 拒绝 | `position_rollback` |
| `pos == committed` | 幂等成功 | — |
| `pos >` 已真正交付给本订阅者的最大位点 | 拒绝 | `position_ahead` |
| `(committed, pos]` 区间内含背压丢弃位点（见 §4） | 拒绝 | `position_gap` |
| 订阅/Broker 已关闭后 ACK | 拒绝 | `subscription_closed` / `broker_closed` |

- **同一订阅者绝不重复交付**：任何 `Position <= committed` 的迟到/重复消息在入队
  与出队两处都会被去重吸收（计入 `Counters.Duplicates`），不会二次进入投递通道。

### 3.2 断线续传

- 订阅者应持久化 `CommittedPosition()`；"断线"即 `Unsubscribe(id)`（或旧订阅 `Close()`）。
- 重连时用**同一 id** 和 `resumeFrom = 上次已确认位点` 调用 `Subscribe`：
  Broker 从续传日志按位点顺序重放 `Position > resumeFrom` 且命中模式的历史消息，再接新消息。
  历史重放与新消息在同一有序临界区内入队，顺序确定（先历史、后新）。
- **已确认的消息不会重复交付，未确认的消息不会丢失**（在保留窗口内）。
- 续传位点本身也被校验：

| resumeFrom 情况 | 结果 | RejectReason |
| --- | --- | --- |
| 早于保留窗口最早可用位点（日志已裁剪） | 拒绝 | `position_expired` |
| 大于 Broker 当前最新位点 | 拒绝 | `position_ahead` |
| `oldest-1`（恰好不丢任何现存消息） | 接受 | — |
| `0`（新订阅者，从头开始） | 接受 | — |

- 同 id 且旧订阅仍存活时重复订阅返回 `duplicate_subscriber`（避免两个读者抢同一流）。

---

## 4. 背压处理策略

并发模型保证**慢订阅者不阻塞发布方与其他订阅者**：

- 发布路径只把消息做 O(1) 追加到每个匹配订阅者的**无界 inbox（FIFO 链表）**，临界区极短；
- 每个订阅者有且仅有一个独立的 dispatch goroutine，把 inbox 转入**有界投递通道**
  （容量为订阅时的 `queueDepth`）；某个订阅者卡住只影响它自己。

队列超限时按订阅时选定的策略确定处理，统计可区分（`Subscription.Counters()`）：

| 策略 | 满队列行为 | 适用 | 丢弃统计 |
| --- | --- | --- | --- |
| `block` | dispatch 等待空位（只阻塞本订阅者）；Broker 关闭立即解除 | 需要不丢、可承受延迟 | 不丢弃 |
| `drop_newest` | 丢弃**最新**消息，保留旧的连续段 | 保护旧数据有序续传 | `DroppedNewest` |
| `drop_oldest` | 挤出通道中**最旧**未确认消息，放入新消息 | 实时值优先 | `DroppedOldest` |

统计字段：`Enqueued / Delivered / DroppedNewest / DroppedOldest / Duplicates / RejectedClosed`。

**丢弃绝不等于静默丢失**：被丢弃的消息仍保留在 Broker 续传日志中，且其位点被专门记录：

- `drop_newest`：消费方见过的位点始终连续，丢弃点位于已交付上界之后，越界确认得到 `position_ahead`；
- `drop_oldest`：可能在已见过的位点区间内留下"洞"，跨越洞确认得到 `position_gap`，
  错误信息会给出**第一个缺口位点**并提示从当前 `committed` 重连续传补齐。

补齐路径：`Unsubscribe` → 以同一 id、`resumeFrom = committed` 重新 `Subscribe`，
被丢弃的消息会被重放，补齐后即可正常 ACK（见测试 `TestGapRefillByResume`）。

---

## 5. 并发与关闭语义

- `Publish / Subscribe / Unsubscribe / Close / Ack` 全部可并发调用，内部加锁；
  锁顺序单向（broker 锁 → 订阅锁），不存在反向获取，因此无死锁环。
- 位点分配与入队在同一有序临界区内完成，保证订阅者看到的位点严格递增，
  不会因并发发布而乱序（避免假的"位点回退"）。
- `Close` 幂等、可并发重复调用。关闭后未完成消息的去留是**确定语义**：

| 消息所处位置 | 关闭后命运 |
| --- | --- |
| 已进入订阅者投递通道缓冲 | **保留**，消费方可读尽，随后 `Deliveries()` 通道关闭 |
| 仍在 inbox 待 dispatch / dispatch 阻塞途中 | **确定作废**，不再补发 |
| 关闭之后新 `Publish` | 拒绝，`broker_closed` |
| 关闭之后新 `Subscribe` | 拒绝，`broker_closed` |
| 关闭之后对存活订阅 `Ack` | 拒绝，`subscription_closed` |
| 已 ACK 的位点 | 不随关闭改变（由订阅者持有，重连可继续使用） |

> Broker 为进程内组件，`Close` 即生命周期终点（不支持重开）；持久化续传位点是订阅者的责任。

---

## 6. API 速览

```go
b := pubsub.NewBroker(pubsub.WithRetention(0)) // 0=续传日志无限保留（默认）

sub, _ := b.Subscribe(ctx, "robot-01",
    pubsub.MustParsePattern("robot.arm.#"),
    8, pubsub.OverflowBlock, 0 /*resumeFrom*/)

pos, err := b.Publish(ctx, pubsub.Message{
    Topic:    pubsub.MustParseTopic("robot.arm.j1.temp"),
    Seq:      1,
    Version:  pubsub.CurrentVersion,
    Producer: "sensor-a",
    Payload:  []byte(`{"c":36.5}`),
})

for d := range sub.Deliveries() {
    // 处理 d.Message ...
    if err := sub.Ack(d.Position); err != nil {
        var re *pubsub.RejectError
        if errors.As(err, &re) { /* 按 re.Reason 区分处理 */ }
    }
}

// 断线：committed := sub.CommittedPosition(); b.Unsubscribe("robot-01")
// 重连：b.Subscribe(ctx, "robot-01", pattern, depth, strategy, committed)
```

拒绝原因全集见 `errors.go` 中的 `RejectReason` 常量；可用 `pubsub.AsRejectError(err)` 提取。

---

## 7. 本地验证方法（无需任何外部服务）

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 常规测试（日志打印 seq / position / committed / 判定原因）
go test -v ./...

# 并发竞态检测（要求通过）
go test -race ./...

# 高重复度压测，进一步暴露时序问题
go test -race -count=20 ./...

# 覆盖率
go test -cover ./...
```

测试覆盖场景：

- **主题匹配**：`*`/`#` 边界、非法主题与模式（`topic_test.go`）；
- **版本/序列号**：未知/不兼容/零版本拒绝、生产者重复与回退拒绝（`message_test.go`、
  `broker_basic_test.go`、`lifecycle_test.go`）；
- **并发收发**：多生产者 + 多订阅者 + 反复订退 + 并发关闭（`TestConcurrentLifecycle`）；
- **断线续传**：从 committed 续传、ACK 回退/越前、续传位点过期/越前（`resume_test.go`）；
- **背压超限**：慢订阅者不阻塞他人、`drop_newest`/`drop_oldest` 统计区分、
  gap 拒绝与重连续传补齐（`backpressure_test.go`）；
- **关闭语义**：已缓冲消息保留可读尽、inbox 在途消息确定作废、关闭后操作被明确拒绝
  （`lifecycle_test.go`、`edge_test.go`）。

测试日志中会打印每条消息的 `seq`、全局 `position`、订阅者 `committed` 位点以及
最终判定（accepted / REJECTED reason=...），便于核对投递结果与判定依据。
