package middleware

// Subscription 表示一个订阅。消息经 C() 交付；消费后调用 Ack 推进确认位点。
type Subscription struct {
	// TODO: 队列、位点、统计、生命周期状态
}

// C 返回消息交付通道。订阅退订或 Broker 关闭后排空即关闭。
func (s *Subscription) C() <-chan Message {
	return nil // TODO: 实现
}

// Ack 确认序列号 seq 之前的消息。位点回退返回 ErrPositionRegression。
func (s *Subscription) Ack(seq uint64) error {
	return nil // TODO: 实现
}

// Position 返回当前已确认位点。
func (s *Subscription) Position() uint64 {
	return 0 // TODO: 实现
}

// Stats 返回该订阅的可区分统计快照。
func (s *Subscription) Stats() Stats {
	return Stats{} // TODO: 实现
}

// Unsubscribe 退订：通道在已入队消息排空后关闭，幂等。
func (s *Subscription) Unsubscribe() {
	// TODO: 实现
}
