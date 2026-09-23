package pubsub_test

import (
	"context"
	"fmt"

	pubsub "github.com/lisama569/robot-pubsub-middleware-go"
)

// Example_fullLifecycle 演示：通配订阅 → 发布（带序列号/版本）→ 按位点确认 →
// 断线后从已确认位点续传，整个过程不依赖任何外部服务。
func Example_fullLifecycle() {
	ctx := context.Background()
	b := pubsub.NewBroker()
	defer b.Close()

	sub, err := b.Subscribe(ctx, "robot-01",
		pubsub.MustParsePattern("robot.arm.#"),
		8, pubsub.OverflowBlock, 0)
	if err != nil {
		panic(err)
	}

	// 发布两条消息。
	for seq := uint64(1); seq <= 2; seq++ {
		topic := pubsub.MustParseTopic("robot.arm.joint1.temp")
		msg := pubsub.Message{
			Topic: topic, Seq: seq, Version: pubsub.CurrentVersion,
			Producer: "sensor-a", Payload: []byte("36.5"),
		}
		if _, err := b.Publish(ctx, msg); err != nil {
			panic(err)
		}
	}

	// 收到第 1 条并确认；第 2 条未确认（模拟断线）。
	d1 := <-sub.Deliveries()
	fmt.Printf("delivered pos=%d seq=%d\n", d1.Position, d1.Message.Seq)
	if err := sub.Ack(d1.Position); err != nil {
		panic(err)
	}
	committed := sub.CommittedPosition()
	_ = b.Unsubscribe("robot-01") // 断线

	// 重连：从已确认位点续传，只拿到未确认的第 2 条。
	sub2, err := b.Subscribe(ctx, "robot-01",
		pubsub.MustParsePattern("robot.arm.#"),
		8, pubsub.OverflowBlock, committed)
	if err != nil {
		panic(err)
	}
	d2 := <-sub2.Deliveries()
	fmt.Printf("resumed pos=%d seq=%d from committed=%d\n", d2.Position, d2.Message.Seq, committed)
	// Output:
	// delivered pos=1 seq=1
	// resumed pos=2 seq=2 from committed=1
}
