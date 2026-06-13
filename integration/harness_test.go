package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// defaultTimeout bounds how long a test waits for a message to arrive.
const defaultTimeout = 10 * time.Second

var nameCounter atomic.Uint64

// brokerURL returns the AMQP URL of the broker started by TestMain, skipping the
// test when no broker is available (e.g. Docker is not running).
func brokerURL(t *testing.T) string {
	t.Helper()
	if testBrokerURL == "" {
		t.Skip("no RabbitMQ broker available; Docker is required for testcontainers " +
			"(or set RABBITMQ_TEST_URL to an existing broker)")
	}
	return testBrokerURL
}

// uniqueName returns a collision-free name for a per-test exchange or queue.
func uniqueName(prefix string) string {
	return fmt.Sprintf("rmqtest.%s.%d.%d", prefix, time.Now().UnixNano(), nameCounter.Add(1))
}

// adminConn opens a raw AMQP connection for test setup/verification and
// registers its cleanup.
func adminConn(t *testing.T, url string) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// adminChannel opens a channel on conn and registers its cleanup.
func adminChannel(t *testing.T, conn *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	return ch
}

// topology bundles the names used by a single test so cleanup is centralised.
type topology struct {
	exchange   string
	queue      string
	routingKey string
}

// newTopology allocates unique names and schedules best-effort broker cleanup.
func newTopology(t *testing.T, conn *amqp.Connection) topology {
	t.Helper()
	topo := topology{
		exchange:   uniqueName("ex"),
		queue:      uniqueName("q"),
		routingKey: uniqueName("rk"),
	}
	// Each deletion runs on its own channel: deleting an entity a given test
	// never created raises a channel exception, which must not cascade to the
	// other deletions.
	safeDelete := func(do func(ch *amqp.Channel)) {
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer func() { _ = ch.Close() }()
		do(ch)
	}
	t.Cleanup(func() {
		safeDelete(
			func(ch *amqp.Channel) { _, _ = ch.QueueDelete(topo.queue, false, false, false) },
		)
		safeDelete(
			func(ch *amqp.Channel) { _, _ = ch.QueueDelete(topo.queue+".dlq", false, false, false) },
		)
		safeDelete(func(ch *amqp.Channel) { _ = ch.ExchangeDelete(topo.exchange, false, false) })
		safeDelete(
			func(ch *amqp.Channel) { _ = ch.ExchangeDelete(topo.exchange+".dlx", false, false) },
		)
	})
	return topo
}

// bindQueue declares a transient queue bound to exchange with routingKey and
// returns its name. Used to observe what a Publisher actually sent.
func bindQueue(t *testing.T, ch *amqp.Channel, exchange, routingKey, queue string) {
	t.Helper()
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare observer queue: %v", err)
	}
	if err := ch.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("bind observer queue: %v", err)
	}
}

// getMessage polls queue for a single message until defaultTimeout.
func getMessage(
	t *testing.T,
	ch *amqp.Channel,
	queue string,
) (amqp.Delivery, bool) {
	t.Helper()
	deadline := time.Now().Add(defaultTimeout)
	for time.Now().Before(deadline) {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get from %s: %v", queue, err)
		}
		if ok {
			return msg, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return amqp.Delivery{}, false
}

// queueDepth reports the number of ready messages in queue via a passive
// declare.
func queueDepth(t *testing.T, conn *amqp.Connection, queue string) int {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("passive declare %s: %v", queue, err)
	}
	return q.Messages
}

// queueExists reports whether a queue is present, without failing the test.
func queueExists(conn *amqp.Connection, name string) bool {
	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	defer func() { _ = ch.Close() }()
	_, err = ch.QueueDeclarePassive(name, true, false, false, false, nil)
	return err == nil
}

// queueHasConsumer reports whether a queue exists and has at least one active
// consumer. A Consumer declares the queue, binds it, and then starts consuming,
// so a registered consumer guarantees the binding exists and publishes are
// routable; the bare queueExists check races the bind and can lose messages.
func queueHasConsumer(conn *amqp.Connection, name string) bool {
	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	return err == nil && q.Consumers > 0
}

// publish sends a single persistent message straight to an exchange, bypassing
// the library so consumer tests are isolated from the publisher.
func publish(t *testing.T, conn *amqp.Connection, exchange, routingKey string, body []byte) {
	t.Helper()
	ch := adminChannel(t, conn)
	err := ch.PublishWithContext(
		context.Background(),
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{ContentType: "application/json", Body: body, DeliveryMode: amqp.Persistent},
	)
	if err != nil {
		t.Fatalf("publish to %s: %v", exchange, err)
	}
}

// waitFor polls cond until it returns true or defaultTimeout elapses.
func waitFor(t *testing.T, cond func() bool) bool {
	t.Helper()
	return waitForCond(t, defaultTimeout, cond)
}

// waitForCond polls cond until it returns true or the given timeout elapses. It
// is used by recovery tests that need longer than defaultTimeout (reconnect and
// consumer-restart backoffs can add several seconds).
func waitForCond(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return cond()
}

// waitForBody waits until want is received on ch or timeout elapses, discarding
// any other values seen in the meantime.
func waitForBody(t *testing.T, ch <-chan string, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case got := <-ch:
			if got == want {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// testContext returns a context cancelled when the test ends.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}
