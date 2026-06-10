package rabbitmq

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNormalizePublishMessageDefaultsAndPreservesProperties(t *testing.T) {
	p := &publisher{cfg: Config{Exchange: "events", DefaultContentType: "application/json"}}
	now := time.Now()
	msg := p.normalizePublishMessage(PublishMessage{
		RoutingKey:      "user.created",
		Body:            []byte("body"),
		ContentEncoding: "gzip",
		Priority:        4,
		CorrelationID:   "corr",
		ReplyTo:         "reply",
		Expiration:      "1000",
		MessageID:       "msg-id",
		Timestamp:       now,
		Type:            "event",
		UserID:          "guest",
		AppID:           "app",
		Mandatory:       true,
		WaitForConfirm:  true,
	})

	if msg.Exchange != "events" {
		t.Errorf("Exchange = %q, want configured exchange", msg.Exchange)
	}
	if msg.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want default", msg.ContentType)
	}
	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, want persistent", msg.DeliveryMode)
	}
	if msg.ContentEncoding != "gzip" || msg.CorrelationID != "corr" || msg.MessageID != "msg-id" {
		t.Fatalf("properties were not preserved: %+v", msg)
	}
	if msg.Headers == nil {
		t.Fatal("Headers = nil, want initialized table")
	}
}

func TestNormalizePublishMessageDefaultsExchange(t *testing.T) {
	p := &publisher{cfg: Config{Exchange: "events"}.normalize()}
	msg := p.normalizePublishMessage(PublishMessage{RoutingKey: "rk", Body: []byte("b")})
	if msg.Exchange != "events" {
		t.Errorf("Exchange = %q, want default %q", msg.Exchange, "events")
	}
	if msg.ContentType != defaultContentType {
		t.Errorf("ContentType = %q, want %q", msg.ContentType, defaultContentType)
	}
}

func TestPublishReturnedErrorMessage(t *testing.T) {
	err := &PublishReturnedError{Returned: ReturnedMessage{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		Exchange:   "events",
		RoutingKey: "user.created",
	}}
	got := err.Error()
	for _, want := range []string{"312", "NO_ROUTE", "events", "user.created"} {
		if !strings.Contains(got, want) {
			t.Errorf("Error() = %q, want it to contain %q", got, want)
		}
	}
}

func TestPublisherClosedGuards(t *testing.T) {
	p := &publisher{}
	p.closed.Store(true)

	if err := p.Publish(context.Background(), "ex", "rk", []byte("x"), false); err == nil {
		t.Error("Publish on a closed publisher should error")
	}
	if err := p.PublishMessage(context.Background(), PublishMessage{RoutingKey: "rk"}); err == nil {
		t.Error("PublishMessage on a closed publisher should error")
	}
	if _, err := p.GetChannel(); err == nil {
		t.Error("GetChannel on a closed publisher should error")
	}
}

func TestPublisherGetChannelWithoutConnection(t *testing.T) {
	p := &publisher{cfg: Config{Exchange: "ex"}.normalize(), log: Config{}.logger()}
	// No pool and no connection: GetChannel falls through to channel creation,
	// which fails because there is no usable connection.
	if _, err := p.GetChannel(); err == nil {
		t.Error("GetChannel without a connection should error")
	}
}

func TestPublisherReturnChannelNilIsSafe(t *testing.T) {
	p := &publisher{}
	p.ReturnChannel(nil) // must not panic
}

func TestPublisherCloseIsIdempotent(t *testing.T) {
	p := &publisher{closeChan: make(chan struct{}), log: Config{}.logger()}
	p.Close()
	p.Close() // second call must be a no-op (not a double close-of-channel panic)
	if !p.closed.Load() {
		t.Error("publisher should be marked closed after Close")
	}
}

func TestNewPublisherValidatesConfig(t *testing.T) {
	if _, err := NewPublisher(Config{}); err == nil {
		t.Error("NewPublisher with an empty config should error")
	}
}

func TestNewPublisherReturnsDialError(t *testing.T) {
	_, err := NewPublisher(Config{
		URL:               "amqp://guest:guest@127.0.0.1:1/",
		Exchange:          "ex",
		DialTimeout:       200 * time.Millisecond,
		ConnectionTimeout: time.Second,
	})
	if err == nil {
		t.Error("NewPublisher against an unreachable broker should error")
	}
}
