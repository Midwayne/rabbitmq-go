package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
)

// startConsumer creates a Consumer, starts consuming the topology's queue in a
// goroutine, and waits until the queue has been declared so publishes are
// routable. The Consumer is closed on test cleanup.
func startConsumer(
	t *testing.T,
	url string,
	cfg rabbitmq.Config,
	topo topology,
	handler rabbitmq.Handler,
) {
	t.Helper()

	cfg.URL = url
	cfg.Exchange = topo.exchange

	consumer, err := rabbitmq.NewConsumer(testContext(t), cfg)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	go func() { _ = consumer.Consume(topo.queue, topo.routingKey, handler) }()

	conn := adminConn(t, url)
	if !waitFor(t, func() bool { return queueHasConsumer(conn, topo.queue) }) {
		t.Fatal("consumer did not start consuming its queue in time")
	}
}

func TestConsumerProcessesMessage(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	received := make(chan string, 1)
	handler := func(ctx context.Context, msg rabbitmq.Message) error {
		received <- string(msg.Body)
		return nil
	}

	startConsumer(t, url, rabbitmq.Config{}, topo, handler)

	publish(t, conn, topo.exchange, topo.routingKey, []byte(`{"id":42}`))

	select {
	case got := <-received:
		if got != `{"id":42}` {
			t.Errorf("handler body = %q, want %q", got, `{"id":42}`)
		}
	case <-testContext(t).Done():
		t.Fatal("handler was not invoked")
	}

	// The message must be acked, leaving the queue empty.
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == 0 }) {
		t.Errorf("queue depth = %d, want 0 (message not acked)", queueDepth(t, conn, topo.queue))
	}
}

func TestConsumerRetriesThenDeadLetters(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	var attempts atomic.Int32
	handler := func(ctx context.Context, msg rabbitmq.Message) error {
		attempts.Add(1)
		return errors.New("always fails")
	}

	startConsumer(t, url, rabbitmq.Config{MaxRetries: 2}, topo, handler)

	publish(t, conn, topo.exchange, topo.routingKey, []byte(`{"id":1}`))

	// Initial delivery (retry 0) + two retries (1, 2) = 3 attempts, then DLQ.
	if !waitFor(t, func() bool { return attempts.Load() == 3 }) {
		t.Errorf("attempts = %d, want 3", attempts.Load())
	}
	dlq := topo.queue + ".dlq"
	if !waitFor(t, func() bool { return queueDepth(t, conn, dlq) == 1 }) {
		t.Errorf("dead-letter queue depth = %d, want 1", queueDepth(t, conn, dlq))
	}
	if depth := queueDepth(t, conn, topo.queue); depth != 0 {
		t.Errorf("main queue depth = %d, want 0", depth)
	}
}

func TestConsumerNonRetryableGoesStraightToDeadLetter(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	var attempts atomic.Int32
	handler := func(ctx context.Context, msg rabbitmq.Message) error {
		attempts.Add(1)
		return rabbitmq.NewNonRetryableError(errors.New("poison message"))
	}

	// MaxRetries is high, but a non-retryable error must skip retries entirely.
	startConsumer(t, url, rabbitmq.Config{MaxRetries: 5}, topo, handler)

	publish(t, conn, topo.exchange, topo.routingKey, []byte(`bad`))

	dlq := topo.queue + ".dlq"
	if !waitFor(t, func() bool { return queueDepth(t, conn, dlq) == 1 }) {
		t.Errorf("dead-letter queue depth = %d, want 1", queueDepth(t, conn, dlq))
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (non-retryable must not retry)", got)
	}
}

func TestConsumerRetryPreservesMessageProperties(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)
	now := time.Now().UTC().Truncate(time.Second)

	retried := make(chan rabbitmq.Message, 1)
	var attempts atomic.Int32
	handler := func(ctx context.Context, msg rabbitmq.Message) error {
		if attempts.Add(1) == 1 {
			return errors.New("retry me")
		}
		retried <- msg
		return nil
	}
	startConsumer(t, url, rabbitmq.Config{MaxRetries: 1}, topo, handler)

	ch := adminChannel(t, conn)
	err := ch.PublishWithContext(
		context.Background(),
		topo.exchange,
		topo.routingKey,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01",
			},
			ContentType:     "application/cloudevents+json",
			ContentEncoding: "gzip",
			DeliveryMode:    amqp.Persistent,
			Priority:        5,
			CorrelationId:   "corr",
			ReplyTo:         "reply",
			Expiration:      "60000",
			MessageId:       "msg-id",
			Timestamp:       now,
			Type:            "user.created",
			AppId:           "app",
			Body:            []byte("body"),
		},
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-retried:
		if got.RetryCount != 1 {
			t.Fatalf("RetryCount = %d, want 1", got.RetryCount)
		}
		if got.ContentType != "application/cloudevents+json" || got.ContentEncoding != "gzip" {
			t.Fatalf("content properties = %q/%q", got.ContentType, got.ContentEncoding)
		}
		if got.Priority != 5 || got.CorrelationID != "corr" || got.ReplyTo != "reply" {
			t.Fatalf("message properties not preserved: %+v", got)
		}
		if got.Headers["traceparent"] == nil {
			t.Fatalf("traceparent header missing after retry: %#v", got.Headers)
		}
	case <-time.After(defaultTimeout):
		t.Fatal("retry delivery was not handled")
	}
}

func TestConsumerRetryUsesOriginalRoutingKeyForTopicWildcard(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)
	topo.routingKey = "user.*"
	originalRoutingKey := "user.created"

	retried := make(chan rabbitmq.Message, 1)
	var attempts atomic.Int32
	handler := func(ctx context.Context, msg rabbitmq.Message) error {
		if attempts.Add(1) == 1 {
			return errors.New("retry me")
		}
		retried <- msg
		return nil
	}
	startConsumer(t, url, rabbitmq.Config{ExchangeType: "topic", MaxRetries: 1}, topo, handler)
	publish(t, conn, topo.exchange, originalRoutingKey, []byte("body"))

	select {
	case got := <-retried:
		if got.RoutingKey != originalRoutingKey {
			t.Fatalf("retried RoutingKey = %q, want %q", got.RoutingKey, originalRoutingKey)
		}
	case <-time.After(defaultTimeout):
		t.Fatal("retry delivery was not handled; retry may have used wildcard binding key")
	}
}

func TestConsumerHealthCheck(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	consumer, err := rabbitmq.NewConsumer(testContext(t), rabbitmq.Config{
		URL:      url,
		Exchange: topo.exchange,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := consumer.Check(context.Background()); err != nil {
		t.Errorf("Check on a live consumer = %v, want nil", err)
	}

	consumer.Close()

	if err := consumer.Check(context.Background()); err == nil {
		t.Error("Check after Close = nil, want an error")
	}
}

func TestConsumerConsumeBodyDeliversBody(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	received := make(chan string, 1)
	consumer, err := rabbitmq.NewConsumer(
		testContext(t),
		rabbitmq.Config{URL: url, Exchange: topo.exchange},
	)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	t.Cleanup(consumer.Close)

	go func() {
		_ = consumer.ConsumeBody(
			topo.queue,
			topo.routingKey,
			func(_ context.Context, body []byte) error {
				received <- string(body)
				return nil
			},
		)
	}()
	if !waitFor(t, func() bool { return queueHasConsumer(conn, topo.queue) }) {
		t.Fatal("consumer did not start consuming its queue in time")
	}

	publish(t, conn, topo.exchange, topo.routingKey, []byte("hello-body"))
	if !waitForBody(t, received, "hello-body", defaultTimeout) {
		t.Fatal("ConsumeBody handler did not receive the body")
	}
}

func TestConsumerMaxRetriesZeroDeadLettersImmediately(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	var attempts atomic.Int32
	handler := func(context.Context, rabbitmq.Message) error {
		attempts.Add(1)
		return errors.New("always fails")
	}
	startConsumer(t, url, rabbitmq.Config{MaxRetries: 0}, topo, handler)

	publish(t, conn, topo.exchange, topo.routingKey, []byte(`{"id":1}`))

	dlq := topo.queue + ".dlq"
	if !waitFor(t, func() bool { return queueDepth(t, conn, dlq) == 1 }) {
		t.Errorf("dead-letter queue depth = %d, want 1", queueDepth(t, conn, dlq))
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (MaxRetries 0 must dead-letter on first failure)", got)
	}
}

func TestConsumerCustomDeadLetterSuffixes(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	customDLQ := topo.queue + ".failed"
	customDLX := topo.exchange + ".dead"
	t.Cleanup(func() {
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer func() { _ = ch.Close() }()
		_, _ = ch.QueueDelete(customDLQ, false, false, false)
		_ = ch.ExchangeDelete(customDLX, false, false)
	})

	handler := func(context.Context, rabbitmq.Message) error { return errors.New("fail") }
	startConsumer(t, url, rabbitmq.Config{
		MaxRetries:               0,
		DeadLetterExchangeSuffix: ".dead",
		DeadLetterQueueSuffix:    ".failed",
	}, topo, handler)

	publish(t, conn, topo.exchange, topo.routingKey, []byte("x"))

	if !waitFor(t, func() bool { return queueDepth(t, conn, customDLQ) == 1 }) {
		t.Errorf(
			"custom dead-letter queue %q depth = %d, want 1",
			customDLQ,
			queueDepth(t, conn, customDLQ),
		)
	}
}

func TestConsumerDisableDeadLetterDropsAfterRetries(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	var attempts atomic.Int32
	handler := func(context.Context, rabbitmq.Message) error {
		attempts.Add(1)
		return errors.New("fail")
	}
	startConsumer(t, url, rabbitmq.Config{MaxRetries: 1, DisableDeadLetter: true}, topo, handler)

	publish(t, conn, topo.exchange, topo.routingKey, []byte("x"))

	// Initial delivery (retry 0) + one retry (1) = 2 attempts, then dropped.
	if !waitFor(t, func() bool { return attempts.Load() == 2 }) {
		t.Errorf("attempts = %d, want 2", attempts.Load())
	}
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == 0 }) {
		t.Errorf(
			"main queue depth = %d, want 0 (message should be dropped)",
			queueDepth(t, conn, topo.queue),
		)
	}
	if queueExists(conn, topo.queue+".dlq") {
		t.Error("dead-letter queue exists, want none when DisableDeadLetter is set")
	}
}

func TestConsumerCompetingConsumersShareQueue(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	const total = 50
	var (
		mu        sync.Mutex
		seen      = make(map[string]int, total)
		processed atomic.Int32
	)
	handler := func(_ context.Context, msg rabbitmq.Message) error {
		mu.Lock()
		seen[string(msg.Body)]++
		mu.Unlock()
		processed.Add(1)
		return nil
	}

	// Two competing consumers (separate connections) on the same queue. A finite
	// prefetch lets the broker distribute deliveries across both.
	startConsumer(t, url, rabbitmq.Config{PrefetchCount: 5}, topo, handler)
	startConsumer(t, url, rabbitmq.Config{PrefetchCount: 5}, topo, handler)

	for i := range total {
		publish(t, conn, topo.exchange, topo.routingKey, fmt.Appendf(nil, "m-%d", i))
	}

	if !waitForCond(t, 20*time.Second, func() bool { return processed.Load() >= total }) {
		t.Fatalf("processed = %d, want >= %d", processed.Load(), total)
	}

	mu.Lock()
	distinct := len(seen)
	mu.Unlock()
	if distinct != total {
		t.Errorf("distinct messages processed = %d, want %d", distinct, total)
	}
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == 0 }) {
		t.Errorf("queue depth = %d, want 0", queueDepth(t, conn, topo.queue))
	}
}

func TestNewConsumerExchangeTypeConflictFails(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	ch := adminChannel(t, conn)
	if err := ch.ExchangeDeclare(
		topo.exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		t.Fatalf("pre-declare exchange: %v", err)
	}

	if _, err := rabbitmq.NewConsumer(testContext(t), rabbitmq.Config{
		URL:          url,
		Exchange:     topo.exchange,
		ExchangeType: "topic",
	}); err == nil {
		t.Fatal("NewConsumer with a conflicting exchange type should fail")
	}
}

func TestNewConsumerHandlesNilContext(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	//nolint:staticcheck // Regression test for nil context guardrail.
	consumer, err := rabbitmq.NewConsumer(nil, rabbitmq.Config{URL: url, Exchange: topo.exchange})
	if err != nil {
		t.Fatalf("NewConsumer with nil context: %v", err)
	}
	consumer.Close()
}
