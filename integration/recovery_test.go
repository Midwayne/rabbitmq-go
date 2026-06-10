package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
)

// TestPublisherRecoversAfterConnectionDrop forces the broker to drop the
// publisher's connection and verifies the publisher reconnects and resumes
// delivering confirmed messages without losing already-confirmed ones.
func TestPublisherRecoversAfterConnectionDrop(t *testing.T) {
	url := brokerURL(t)
	mgmt := managementClient(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)
	connName := uniqueName("pub-recover")

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:                 url,
		Exchange:            topo.exchange,
		ConnectionName:      connName,
		ReconnectMaxBackoff: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	// A confirmed publish lands before the drop and must survive it (durable
	// queue + persistent delivery).
	if err := pub.Publish(
		testContext(t),
		topo.exchange,
		topo.routingKey,
		[]byte("before"),
		true,
	); err != nil {
		t.Fatalf("baseline publish: %v", err)
	}

	if !mgmt.waitForNamedConnection(t, connName) {
		t.Fatal("publisher connection never appeared in the management API")
	}
	if closed := mgmt.closeNamedConnections(t, connName); closed == 0 {
		t.Fatal("no publisher connection was closed")
	}

	// After the drop the publisher reconnects; publishes succeed again. Transient
	// failures during the reconnect window are expected, so we poll.
	if !waitForCond(t, 20*time.Second, func() bool {
		return pub.Publish(
			context.Background(),
			topo.exchange,
			topo.routingKey,
			[]byte("after"),
			true,
		) == nil
	}) {
		t.Fatal("publisher did not recover after the connection drop")
	}

	// The pre-drop message and at least one recovered message are both enqueued.
	if !waitFor(t, func() bool { return queueDepth(t, conn, topo.queue) >= 2 }) {
		t.Errorf(
			"queue depth = %d, want >= 2 (pre-drop + recovered)",
			queueDepth(t, conn, topo.queue),
		)
	}
}

// TestPublisherReconnectWhilePublishingRace hammers the publisher from several
// goroutines while repeatedly force-dropping its connection. Run under
// `go test -race`, it exercises the channel pool's reconnect/borrow/return paths
// concurrently and asserts every confirmed publish is durably enqueued (no
// confirmed message is lost across reconnects).
func TestPublisherReconnectWhilePublishingRace(t *testing.T) {
	url := brokerURL(t)
	mgmt := managementClient(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)
	connName := uniqueName("pub-race")

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:                   url,
		Exchange:              topo.exchange,
		ConnectionName:        connName,
		ChannelPoolSize:       4,
		ReconnectMaxBackoff:   300 * time.Millisecond,
		PublishConfirmTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, conn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	var (
		stop      atomic.Bool
		succeeded atomic.Int64
		wg        sync.WaitGroup
	)
	const writers = 6
	for range writers {
		wg.Go(func() {
			for !stop.Load() {
				// Only confirmed (acked) publishes are counted; transient errors
				// during a reconnect are expected and ignored.
				if pub.Publish(
					context.Background(),
					topo.exchange,
					topo.routingKey,
					[]byte("x"),
					true,
				) == nil {
					succeeded.Add(1)
				}
			}
		})
	}

	// Force several reconnects while the writers run.
	for round := range 3 {
		if !mgmt.waitForNamedConnection(t, connName) {
			stop.Store(true)
			wg.Wait()
			t.Fatalf("round %d: publisher connection not visible to management API", round)
		}
		mgmt.closeNamedConnections(t, connName)
		// Let the publisher reconnect (initial backoff ~1s) and resume publishing
		// before the next drop.
		time.Sleep(1500 * time.Millisecond)
	}

	stop.Store(true)
	wg.Wait()

	if succeeded.Load() == 0 {
		t.Fatal("no publishes succeeded across the reconnects")
	}
	// Every confirmed publish must be durably enqueued: depth must reach the
	// confirmed count (nothing consumes this queue).
	if !waitFor(
		t,
		func() bool { return queueDepth(t, conn, topo.queue) >= int(succeeded.Load()) },
	) {
		t.Errorf(
			"queue depth = %d, want >= %d confirmed publishes (confirmed message lost across reconnect)",
			queueDepth(t, conn, topo.queue),
			succeeded.Load(),
		)
	}
}

// TestConsumerRecoversAfterConnectionDrop drops the consumer's connection and
// verifies it reconnects, re-declares its topology, and processes messages
// published during and after the outage.
func TestConsumerRecoversAfterConnectionDrop(t *testing.T) {
	url := brokerURL(t)
	mgmt := managementClient(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)
	connName := uniqueName("cons-recover")

	received := make(chan string, 16)
	handler := func(_ context.Context, msg rabbitmq.Message) error {
		received <- string(msg.Body)
		return nil
	}
	startConsumer(t, url, rabbitmq.Config{ConnectionName: connName}, topo, handler)

	// Baseline: a message flows through before the drop.
	publish(t, conn, topo.exchange, topo.routingKey, []byte("before"))
	if !waitForBody(t, received, "before", defaultTimeout) {
		t.Fatal("baseline message was not consumed")
	}

	if !mgmt.waitForNamedConnection(t, connName) {
		t.Fatal("consumer connection never appeared in the management API")
	}
	if closed := mgmt.closeNamedConnections(t, connName); closed == 0 {
		t.Fatal("no consumer connection was closed")
	}

	// Publish after the drop. The durable queue retains the message until the
	// consumer reconnects and re-subscribes (reconnect + restart backoff can take
	// several seconds), after which it is processed.
	publish(t, conn, topo.exchange, topo.routingKey, []byte("after"))
	if !waitForBody(t, received, "after", 30*time.Second) {
		t.Fatal("consumer did not process a message after the connection drop")
	}
}
