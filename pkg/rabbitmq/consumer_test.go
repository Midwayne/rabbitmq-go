package rabbitmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConsumeRejectsNilHandler(t *testing.T) {
	c := &Consumer{ctx: context.Background()}
	if err := c.Consume("q", "rk", nil); err == nil {
		t.Fatal("Consume nil handler = nil, want error")
	}
}

func TestConsumeBodyRejectsNilHandler(t *testing.T) {
	c := &Consumer{ctx: context.Background()}
	if err := c.ConsumeBody("q", "rk", nil); err == nil {
		t.Fatal("ConsumeBody nil handler = nil, want error")
	}
}

func TestConsumeConcurrentRejectsNilHandler(t *testing.T) {
	c := &Consumer{ctx: context.Background()}
	if err := c.ConsumeConcurrent("q", "rk", 2, nil); err == nil {
		t.Fatal("ConsumeConcurrent nil handler = nil, want error")
	}
}

func TestConsumeBodyConcurrentRejectsNilHandler(t *testing.T) {
	c := &Consumer{ctx: context.Background()}
	if err := c.ConsumeBodyConcurrent("q", "rk", 2, nil); err == nil {
		t.Fatal("ConsumeBodyConcurrent nil handler = nil, want error")
	}
}

func TestNewConsumerValidatesConfig(t *testing.T) {
	if _, err := NewConsumer(context.Background(), Config{}); err == nil {
		t.Error("NewConsumer with an empty config should error")
	}
}

func TestNewConsumerReturnsDialError(t *testing.T) {
	_, err := NewConsumer(context.Background(), Config{
		URL:               "amqp://guest:guest@127.0.0.1:1/",
		Exchange:          "ex",
		DialTimeout:       200 * time.Millisecond,
		ConnectionTimeout: time.Second,
	})
	if err == nil {
		t.Error("NewConsumer against an unreachable broker should error")
	}
}

func TestConsumerCheckGuards(t *testing.T) {
	var nilConsumer *Consumer
	if err := nilConsumer.Check(context.Background()); err == nil {
		t.Error("Check on a nil consumer should error")
	}
	if err := (&Consumer{}).Check(context.Background()); err == nil {
		t.Error("Check with a nil connection should error")
	}
}

func TestConsumeReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Consumer{ctx: ctx, log: Config{}.logger()}
	if err := c.Consume(
		"q",
		"rk",
		func(context.Context, Message) error { return nil },
	); err != nil {
		t.Errorf("Consume with a cancelled context = %v, want nil", err)
	}
}

func TestConsumeConcurrentReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Consumer{ctx: ctx, log: Config{}.logger()}
	if err := c.ConsumeConcurrent(
		"q",
		"rk",
		2,
		func(context.Context, Message) error { return nil },
	); err != nil {
		t.Errorf("ConsumeConcurrent with a cancelled context = %v, want nil", err)
	}
}

func TestConsumeBodyConcurrentReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Consumer{ctx: ctx, log: Config{}.logger()}
	if err := c.ConsumeBodyConcurrent(
		"q",
		"rk",
		2,
		func(context.Context, []byte) error { return nil },
	); err != nil {
		t.Errorf("ConsumeBodyConcurrent with a cancelled context = %v, want nil", err)
	}
}

func TestConsumeBodyConcurrentFallsBackToSequentialForOneWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Consumer{ctx: ctx, log: Config{}.logger()}
	if err := c.ConsumeBodyConcurrent(
		"q",
		"rk",
		1,
		func(context.Context, []byte) error { return nil },
	); err != nil {
		t.Errorf("ConsumeBodyConcurrent with one worker = %v, want nil", err)
	}
}

func TestCallHandlerRecoversPanic(t *testing.T) {
	c := &Consumer{log: Config{}.logger()}
	err := c.callHandler(
		context.Background(),
		amqp.Delivery{Body: []byte("body")},
		0,
		func(context.Context, Message) error { panic("boom") },
	)
	if err == nil {
		t.Fatal("panic handler error = nil, want error")
	}
}

func TestCallHandlerMapsDeliveryMetadata(t *testing.T) {
	c := &Consumer{log: Config{}.logger()}
	now := time.Now()
	delivery := amqp.Delivery{
		Headers: amqp.Table{
			"traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
		},
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		DeliveryMode:    amqp.Persistent,
		Priority:        7,
		CorrelationId:   "corr",
		ReplyTo:         "reply",
		Expiration:      "1000",
		MessageId:       "msg-id",
		Timestamp:       now,
		Type:            "event",
		UserId:          "guest",
		AppId:           "app",
		Exchange:        "events",
		RoutingKey:      "user.created",
		Redelivered:     true,
		Body:            []byte("body"),
	}

	var got Message
	err := c.callHandler(
		context.Background(),
		delivery,
		3,
		func(_ context.Context, msg Message) error {
			got = msg
			return errors.New("stop")
		},
	)
	if err == nil {
		t.Fatal("handler error = nil, want propagated error")
	}
	if string(got.Body) != "body" || got.RoutingKey != "user.created" || got.Exchange != "events" {
		t.Fatalf("message metadata not mapped: %+v", got)
	}
	if got.ContentEncoding != "gzip" || got.CorrelationID != "corr" || got.MessageID != "msg-id" {
		t.Fatalf("message properties not mapped: %+v", got)
	}
	if got.RetryCount != 3 || !got.Redelivered {
		t.Fatalf(
			"retry/redelivery metadata = retry %d redelivered %v",
			got.RetryCount,
			got.Redelivered,
		)
	}
}

func TestSleepContextCanBeCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepContext(ctx, time.Hour); err == nil {
		t.Fatal("sleepContext canceled = nil, want error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("sleepContext took %v, want prompt return", elapsed)
	}
}

func TestRetryPublishFailureRequeuesOriginal(t *testing.T) {
	acks := &recordingAcknowledger{}
	c := &Consumer{
		cfg: Config{MaxRetries: 1},
		log: Config{}.logger(),
		retryPublish: func(*amqp.Channel, string, amqp.Delivery, int) error {
			return errors.New("retry publish failed")
		},
	}
	delivery := amqp.Delivery{Acknowledger: acks, DeliveryTag: 42, Body: []byte("body")}

	outcome := c.handleHandlerError(
		context.Background(),
		nil,
		"queue",
		"rk",
		delivery,
		0,
		errors.New("handler failed"),
	)

	if outcome.DeadLettered {
		t.Fatal("DeadLettered = true, want false for retry publish failure")
	}
	if !outcome.Nacked || !outcome.Requeued || outcome.Acked {
		t.Fatalf("outcome = %+v, want nacked and requeued without ack", outcome)
	}
	if got := acks.acks.Load(); got != 0 {
		t.Fatalf("acks = %d, want 0", got)
	}
	if got := acks.nacks.Load(); got != 1 {
		t.Fatalf("nacks = %d, want 1", got)
	}
	if !acks.requeue.Load() {
		t.Fatal("nack requeue = false, want true")
	}
}

type recordingAcknowledger struct {
	acks    atomic.Int32
	nacks   atomic.Int32
	rejects atomic.Int32
	requeue atomic.Bool
}

func (r *recordingAcknowledger) Ack(uint64, bool) error {
	r.acks.Add(1)
	return nil
}

func (r *recordingAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	r.nacks.Add(1)
	r.requeue.Store(requeue)
	return nil
}

func (r *recordingAcknowledger) Reject(_ uint64, requeue bool) error {
	r.rejects.Add(1)
	r.requeue.Store(requeue)
	return nil
}
