package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
)

func TestPublisherDeliversConfirmedMessage(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:      url,
		Exchange: topo.exchange,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	// Bind an observer queue so we can see exactly what was published.
	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	body := []byte(`{"hello":"world"}`)
	if err := pub.Publish(testContext(t), topo.exchange, topo.routingKey, body, true); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, ok := getMessage(t, ch, topo.queue)
	if !ok {
		t.Fatal("no message delivered to the bound queue")
	}
	if string(msg.Body) != string(body) {
		t.Errorf("body = %q, want %q", msg.Body, body)
	}
	if msg.ContentType != "application/json" {
		t.Errorf("content type = %q, want application/json", msg.ContentType)
	}
	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode = %d, want persistent (%d)", msg.DeliveryMode, amqp.Persistent)
	}
}

func TestPublisherPublishMessagePreservesProperties(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{URL: url, Exchange: topo.exchange})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)
	now := time.Now().UTC().Truncate(time.Second)
	err = pub.PublishMessage(testContext(t), rabbitmq.PublishMessage{
		Exchange:   topo.exchange,
		RoutingKey: topo.routingKey,
		Body:       []byte("body"),
		Headers: amqp.Table{
			"traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01",
		},
		ContentType:     "application/cloudevents+json",
		ContentEncoding: "gzip",
		DeliveryMode:    amqp.Persistent,
		Priority:        6,
		CorrelationID:   "corr",
		ReplyTo:         "reply",
		Expiration:      "60000",
		MessageID:       "msg-id",
		Timestamp:       now,
		Type:            "user.created",
		AppID:           "app",
		WaitForConfirm:  true,
	})
	if err != nil {
		t.Fatalf("PublishMessage: %v", err)
	}

	msg, ok := getMessage(t, ch, topo.queue)
	if !ok {
		t.Fatal("no message delivered")
	}
	if msg.ContentType != "application/cloudevents+json" || msg.ContentEncoding != "gzip" {
		t.Fatalf("content properties = %q/%q", msg.ContentType, msg.ContentEncoding)
	}
	if msg.Priority != 6 || msg.CorrelationId != "corr" || msg.ReplyTo != "reply" {
		t.Fatalf("message properties not preserved: %+v", msg)
	}
	if msg.Headers["traceparent"] == nil {
		t.Fatalf("traceparent header missing: %#v", msg.Headers)
	}
}

func TestPublisherMandatoryReturnsUnroutableMessage(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{URL: url, Exchange: topo.exchange})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	err = pub.PublishMessage(testContext(t), rabbitmq.PublishMessage{
		Exchange:       topo.exchange,
		RoutingKey:     topo.routingKey,
		Body:           []byte("body"),
		Mandatory:      true,
		WaitForConfirm: true,
	})
	var returned *rabbitmq.PublishReturnedError
	if !errors.As(err, &returned) {
		t.Fatalf("PublishMessage error = %v, want PublishReturnedError", err)
	}
	if returned.Returned.RoutingKey != topo.routingKey ||
		string(returned.Returned.Message.Body) != "body" {
		t.Fatalf("returned message mismatch: %+v", returned.Returned)
	}
}

func TestPublisherPublishToNonDefaultExchange(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{URL: url, Exchange: topo.exchange})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	// A second exchange the caller declares and targets explicitly via
	// PublishMessage.Exchange, distinct from Config.Exchange.
	otherExchange := uniqueName("ex.other")
	otherRK := uniqueName("rk.other")
	otherQueue := uniqueName("q.other")

	ch := adminChannel(t, conn)
	if err := ch.ExchangeDeclare(
		otherExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		t.Fatalf("declare other exchange: %v", err)
	}
	t.Cleanup(func() {
		dch, err := conn.Channel()
		if err != nil {
			return
		}
		defer func() { _ = dch.Close() }()
		_, _ = dch.QueueDelete(otherQueue, false, false, false)
		_ = dch.ExchangeDelete(otherExchange, false, false)
	})
	bindQueue(t, ch, otherExchange, otherRK, otherQueue)

	if err := pub.PublishMessage(testContext(t), rabbitmq.PublishMessage{
		Exchange:       otherExchange,
		RoutingKey:     otherRK,
		Body:           []byte("routed"),
		WaitForConfirm: true,
	}); err != nil {
		t.Fatalf("PublishMessage to non-default exchange: %v", err)
	}

	msg, ok := getMessage(t, ch, otherQueue)
	if !ok || string(msg.Body) != "routed" {
		t.Fatalf("message not delivered to non-default exchange (ok=%v body=%q)", ok, msg.Body)
	}
}

func TestPublisherMandatoryRoutableSucceeds(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{URL: url, Exchange: topo.exchange})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	// A mandatory publish that routes to a bound queue must NOT be returned.
	if err := pub.PublishMessage(testContext(t), rabbitmq.PublishMessage{
		Exchange:       topo.exchange,
		RoutingKey:     topo.routingKey,
		Body:           []byte("ok"),
		Mandatory:      true,
		WaitForConfirm: true,
	}); err != nil {
		t.Fatalf("mandatory routable publish: %v", err)
	}
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == 1 }) {
		t.Errorf("queue depth = %d, want 1", queueDepth(t, conn, topo.queue))
	}
}

func TestPublisherConfirmSucceedsWithoutBoundQueue(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:      url,
		Exchange: topo.exchange,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	// A confirmed publish to an exchange with no bound queue is still acked by
	// the broker (the message is simply dropped). This exercises the deferred
	// confirmation path.
	if err := pub.Publish(
		testContext(t),
		topo.exchange,
		topo.routingKey,
		[]byte("{}"),
		true,
	); err != nil {
		t.Fatalf("confirmed publish failed: %v", err)
	}
}

func TestPublisherWithoutConfirmDelivers(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{URL: url, Exchange: topo.exchange})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	// waitForConfirm=false exercises the fire-and-forget publish path.
	if err := pub.Publish(
		testContext(t),
		topo.exchange,
		topo.routingKey,
		[]byte("noconfirm"),
		false,
	); err != nil {
		t.Fatalf("Publish without confirm: %v", err)
	}
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == 1 }) {
		t.Errorf("queue depth = %d, want 1", queueDepth(t, conn, topo.queue))
	}
}

func TestPublisherAcceptsNilContext(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{URL: url, Exchange: topo.exchange})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	//nolint:staticcheck // Regression test for the nil-context guard on Publish.
	if err := pub.Publish(nil, topo.exchange, topo.routingKey, []byte("a"), true); err != nil {
		t.Fatalf("Publish with nil context: %v", err)
	}
	//nolint:staticcheck // Regression test for the nil-context guard on PublishMessage.
	if err := pub.PublishMessage(nil, rabbitmq.PublishMessage{
		RoutingKey:     topo.routingKey,
		Body:           []byte("b"),
		WaitForConfirm: true,
	}); err != nil {
		t.Fatalf("PublishMessage with nil context: %v", err)
	}
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == 2 }) {
		t.Errorf("queue depth = %d, want 2", queueDepth(t, conn, topo.queue))
	}
}

func TestNewPublisherExchangeTypeConflictFails(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	// Pre-declare the exchange as "direct"; constructing a publisher that wants it
	// as "topic" must fail because the redeclare conflicts.
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

	if _, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:          url,
		Exchange:     topo.exchange,
		ExchangeType: "topic",
	}); err == nil {
		t.Fatal("NewPublisher with a conflicting exchange type should fail")
	}
}

func TestNewPublisherConnectionTimeout(t *testing.T) {
	url := brokerURL(t)
	// A 1ns connection timeout always fires before the AMQP handshake completes,
	// exercising the dial-timeout path (and the late-connection discard).
	if _, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:               url,
		Exchange:          "ex",
		ConnectionTimeout: time.Nanosecond,
	}); err == nil {
		t.Fatal("NewPublisher with a 1ns connection timeout should fail")
	}
}

func TestPublisherConfirmTimeout(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	// A 1ns confirm timeout always elapses before the broker's ack returns,
	// exercising the publish-confirm timeout path.
	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:                   url,
		Exchange:              topo.exchange,
		PublishConfirmTimeout: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(
		testContext(t),
		topo.exchange,
		topo.routingKey,
		[]byte("x"),
		true,
	); err == nil {
		t.Fatal("Publish with a 1ns confirm timeout should fail")
	}
}

func TestPublisherReturnChannelClosesWhenPoolFull(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:             url,
		Exchange:        topo.exchange,
		ChannelPoolSize: 1,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	// Borrow two channels; the idle cache holds at most one.
	ch1, err := pub.GetChannel()
	if err != nil {
		t.Fatalf("GetChannel #1: %v", err)
	}
	ch2, err := pub.GetChannel()
	if err != nil {
		t.Fatalf("GetChannel #2: %v", err)
	}
	if ch1 == ch2 {
		t.Fatal("expected two distinct channels")
	}

	pub.ReturnChannel(ch1) // returns to the idle cache
	pub.ReturnChannel(ch2) // cache full -> channel is closed and discarded

	if !ch2.IsClosed() {
		t.Error("channel returned to a full pool should be closed")
	}
}

func TestPublisherReusesPooledChannels(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:             url,
		Exchange:        topo.exchange,
		ChannelPoolSize: 2,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	const count = 25
	for i := range count {
		if err := pub.Publish(
			testContext(t),
			topo.exchange,
			topo.routingKey,
			[]byte("{}"),
			true,
		); err != nil {
			t.Fatalf("Publish #%d: %v", i, err)
		}
	}

	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == count }) {
		t.Errorf("queue depth = %d, want %d", queueDepth(t, conn, topo.queue), count)
	}
}

func TestPublisherConcurrentPublishRace(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:             url,
		Exchange:        topo.exchange,
		ChannelPoolSize: 2,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	const goroutines = 8
	const perGoroutine = 10
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perGoroutine {
				if err := pub.Publish(
					context.Background(),
					topo.exchange,
					topo.routingKey,
					[]byte("{}"),
					true,
				); err != nil {
					t.Errorf("Publish: %v", err)
				}
			}
		})
	}
	wg.Wait()

	want := goroutines * perGoroutine
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) == want }) {
		t.Errorf("queue depth = %d, want %d", queueDepth(t, conn, topo.queue), want)
	}
}

func TestPublisherMaxChannelAgeUsesCreationTime(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:             url,
		Exchange:        topo.exchange,
		ChannelPoolSize: 1,
		MaxChannelAge:   150 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch1, err := pub.GetChannel()
	if err != nil {
		t.Fatalf("GetChannel #1: %v", err)
	}
	pub.ReturnChannel(ch1)
	time.Sleep(75 * time.Millisecond)
	ch2, err := pub.GetChannel()
	if err != nil {
		t.Fatalf("GetChannel #2: %v", err)
	}
	if ch2 != ch1 {
		t.Fatal("channel was replaced before MaxChannelAge elapsed")
	}
	pub.ReturnChannel(ch2)
	time.Sleep(100 * time.Millisecond)
	ch3, err := pub.GetChannel()
	if err != nil {
		t.Fatalf("GetChannel #3: %v", err)
	}
	defer pub.ReturnChannel(ch3)
	if ch3 == ch1 {
		t.Fatal("channel was reused after total MaxChannelAge elapsed")
	}
}

func TestPublisherDeclareQueueTopologyBuffersBeforeConsumer(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:      url,
		Exchange: topo.exchange,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	if err := pub.DeclareQueueTopology(topo.queue, topo.routingKey); err != nil {
		t.Fatalf("DeclareQueueTopology: %v", err)
	}
	// Redeclaring must be a no-op, not a precondition conflict.
	if err := pub.DeclareQueueTopology(topo.queue, topo.routingKey); err != nil {
		t.Fatalf("DeclareQueueTopology (redeclare): %v", err)
	}

	// With no consumer ever attached, a published message is buffered in the
	// declared queue instead of being dropped as unroutable.
	body := []byte(`{"buffered":true}`)
	if err := pub.Publish(testContext(t), topo.exchange, topo.routingKey, body, true); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ch := adminChannel(t, conn)
	msg, ok := getMessage(t, ch, topo.queue)
	if !ok {
		t.Fatal("message was not buffered in the publisher-declared queue")
	}
	if string(msg.Body) != string(body) {
		t.Errorf("body = %q, want %q", msg.Body, body)
	}

	// A consumer attaching later declares the same topology and must not hit
	// a precondition conflict with the publisher's declaration.
	consumer, err := rabbitmq.NewConsumer(testContext(t), rabbitmq.Config{
		URL:           url,
		Exchange:      topo.exchange,
		PrefetchCount: 1,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	got := make(chan string, 1)
	go func() {
		_ = consumer.Consume(topo.queue, topo.routingKey, func(_ context.Context, msg rabbitmq.Message) error {
			got <- string(msg.Body)
			return nil
		})
	}()

	const second = `{"after":"consumer"}`
	if err := pub.Publish(testContext(t), topo.exchange, topo.routingKey, []byte(second), true); err != nil {
		t.Fatalf("Publish after consumer: %v", err)
	}
	if !waitForBody(t, got, second, defaultTimeout) {
		t.Fatal("consumer did not receive message after redeclaring the topology")
	}
}
