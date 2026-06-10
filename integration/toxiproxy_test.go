package integration

import (
	"context"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"

	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
)

// proxiedBroker returns the AMQP URL routed through Toxiproxy plus a handle to
// the proxy for fault injection. It skips the test when Toxiproxy is
// unavailable, and restores the proxy to a clean, enabled state afterwards so
// tests do not leak faults into each other.
func proxiedBroker(t *testing.T) (string, *toxiproxy.Proxy) {
	t.Helper()
	if testProxiedBrokerURL == "" || testToxiproxyURI == "" {
		t.Skip("Toxiproxy unavailable; fault-injection tests require the testcontainers setup")
	}

	client := toxiproxy.NewClient(testToxiproxyURI)
	proxy, err := client.Proxy(toxiproxyName)
	if err != nil {
		t.Fatalf("get toxiproxy proxy %q: %v", toxiproxyName, err)
	}

	t.Cleanup(func() {
		_ = proxy.Enable()
		if toxics, err := proxy.Toxics(); err == nil {
			for _, tx := range toxics {
				_ = proxy.RemoveToxic(tx.Name)
			}
		}
	})
	return testProxiedBrokerURL, proxy
}

// TestPublisherSurvivesNetworkOutage cuts the network long enough for several
// backed-off reconnect attempts to fail, then restores it and asserts the
// publisher reconnects and resumes delivering. It exercises the reconnect
// retry/backoff loop (not just a single clean drop).
func TestPublisherSurvivesNetworkOutage(t *testing.T) {
	proxiedURL, proxy := proxiedBroker(t)
	// Verify and set up through a direct connection so the outage only affects the
	// publisher (which connects via the proxy).
	directConn := adminConn(t, brokerURL(t))
	topo := newTopology(t, directConn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:                   proxiedURL,
		Exchange:              topo.exchange,
		ReconnectMaxBackoff:   500 * time.Millisecond,
		PublishConfirmTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ch := adminChannel(t, directConn)
	bindQueue(t, ch, topo.exchange, topo.routingKey, topo.queue)

	if err := pub.Publish(
		testContext(t),
		topo.exchange,
		topo.routingKey,
		[]byte("before"),
		true,
	); err != nil {
		t.Fatalf("baseline publish: %v", err)
	}

	// Cut the network: the connection drops and new dials are refused, so the
	// publisher's reconnect attempts fail and back off repeatedly.
	if err := proxy.Disable(); err != nil {
		t.Fatalf("disable proxy: %v", err)
	}
	if err := pub.Publish(
		testContext(t),
		topo.exchange,
		topo.routingKey,
		[]byte("down"),
		true,
	); err == nil {
		t.Error("publish during the outage should fail")
	}
	time.Sleep(2500 * time.Millisecond) // several backed-off reconnect attempts
	if err := proxy.Enable(); err != nil {
		t.Fatalf("enable proxy: %v", err)
	}

	if !waitForCond(t, 20*time.Second, func() bool {
		return pub.Publish(
			context.Background(),
			topo.exchange,
			topo.routingKey,
			[]byte("after"),
			true,
		) == nil
	}) {
		t.Fatal("publisher did not recover after the network outage")
	}
	if !waitFor(t, func() bool { return queueDepth(t, directConn, topo.queue) >= 2 }) {
		t.Errorf(
			"queue depth = %d, want >= 2 (pre-outage + recovered)",
			queueDepth(t, directConn, topo.queue),
		)
	}
}

// TestConsumerSurvivesNetworkOutage cuts the network under a running consumer and
// asserts it reconnects, re-declares its topology, and processes a message
// published during the outage. It exercises the consumer reconnect/restart loop.
func TestConsumerSurvivesNetworkOutage(t *testing.T) {
	proxiedURL, proxy := proxiedBroker(t)
	directConn := adminConn(t, brokerURL(t))
	topo := newTopology(t, directConn)

	received := make(chan string, 16)
	handler := func(_ context.Context, msg rabbitmq.Message) error {
		received <- string(msg.Body)
		return nil
	}
	startConsumer(
		t,
		proxiedURL,
		rabbitmq.Config{ReconnectMaxBackoff: 500 * time.Millisecond},
		topo,
		handler,
	)

	publish(t, directConn, topo.exchange, topo.routingKey, []byte("before"))
	if !waitForBody(t, received, "before", defaultTimeout) {
		t.Fatal("baseline message was not consumed")
	}

	if err := proxy.Disable(); err != nil {
		t.Fatalf("disable proxy: %v", err)
	}
	time.Sleep(2500 * time.Millisecond) // several backed-off reconnect attempts
	if err := proxy.Enable(); err != nil {
		t.Fatalf("enable proxy: %v", err)
	}

	// Published during the outage; the durable queue holds it until the consumer
	// reconnects and re-subscribes (reconnect + restart backoff can take seconds).
	publish(t, directConn, topo.exchange, topo.routingKey, []byte("after"))
	if !waitForBody(t, received, "after", 40*time.Second) {
		t.Fatal("consumer did not recover after the network outage")
	}
}

// TestPublisherChannelCreateTimeoutUnderLatency stalls the connection with a
// latency toxic so opening a fresh channel exceeds ChannelCreateTimeout,
// exercising the channel-creation timeout path and the late-channel cleanup.
func TestPublisherChannelCreateTimeoutUnderLatency(t *testing.T) {
	proxiedURL, proxy := proxiedBroker(t)
	directConn := adminConn(t, brokerURL(t))
	topo := newTopology(t, directConn)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:                  proxiedURL,
		Exchange:             topo.exchange,
		ChannelPoolSize:      1,
		ChannelCreateTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	// Borrow the single warm channel so the next GetChannel must open a new one.
	warm, err := pub.GetChannel()
	if err != nil {
		t.Fatalf("GetChannel (warm): %v", err)
	}

	// Delay server responses well past ChannelCreateTimeout. Removed before
	// pub.Close so teardown is not also slowed (defers run LIFO).
	if _, err := proxy.AddToxic("latency_down", "latency", "downstream", 1.0, toxiproxy.Attributes{
		"latency": 2000,
	}); err != nil {
		t.Fatalf("add latency toxic: %v", err)
	}
	defer func() { _ = proxy.RemoveToxic("latency_down") }()

	if _, err := pub.GetChannel(); err == nil {
		t.Error("GetChannel should time out while channel creation is stalled")
	}

	pub.ReturnChannel(warm)
}
