package pubsub

import "time"

// 测试统一超时通道，避免任何 goroutine 永久挂起导致测试卡死。
func timeout() <-chan time.Time { return time.After(3 * time.Second) }
