// Package pubsub 是一个纯进程内的机器人组件通信中间件，提供：
//
//   - 按点分层级主题与通配模式（'*' 单层、'#' 多层）的发布/订阅；
//   - 携带生产者序列号与线路版本的消息，非法/未知/不兼容版本被明确拒绝；
//   - 基于全局位点与订阅者 ACK 的可靠投递与断线续传；
//   - 每个订阅者独立的有界投递队列与三种确定背压策略；
//   - Publish/Subscribe/Unsubscribe/Close 全并发安全，关闭语义确定。
//
// 任何"没有交付"的情况都不会静默发生：要么消息进入投递通道，要么返回携带
// RejectReason 的 *RejectError（见 errors.go），可由调用方程序化区分处理。
//
// 典型用法参见 Example_fullLifecycle。详细规则见仓库 README.md。
package pubsub
