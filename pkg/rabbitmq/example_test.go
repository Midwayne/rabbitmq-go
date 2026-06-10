package rabbitmq_test

import (
	"context"
	"log/slog"
	"time"

	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
	"github.com/Midwayne/rabbitmq-go/pkg/rabbitmq/logging"
)

// ExampleNewPublisher shows publishing a message and waiting for the broker to
// confirm it.
func ExampleNewPublisher() {
	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:      "amqp://guest:guest@localhost:5672/",
		Exchange: "events",
		Logger:   logging.NewSlogLogger(slog.Default()),
	})
	if err != nil {
		// handle error
		return
	}
	defer pub.Close()

	ctx := context.Background()
	const waitForConfirm = true
	_ = pub.Publish(ctx, "events", "user.created", []byte(`{"id":1}`), waitForConfirm)
}

// ExampleNewConsumer shows consuming a queue with a bounded retry policy.
func ExampleNewConsumer() {
	ctx := context.Background()

	consumer, err := rabbitmq.NewConsumer(ctx, rabbitmq.Config{
		URL:           "amqp://guest:guest@localhost:5672/",
		Exchange:      "events",
		PrefetchCount: 10,
		MaxRetries:    3,
	})
	if err != nil {
		// handle error
		return
	}
	defer consumer.Close()

	handler := func(ctx context.Context, msg rabbitmq.Message) error {
		// Process the message. Returning nil acks it; returning an error
		// retries it; wrapping the error with rabbitmq.NewNonRetryableError
		// dead-letters it immediately.
		return nil
	}

	// Consume blocks until ctx is cancelled, so run it in its own goroutine.
	go func() { _ = consumer.Consume("events-worker", "user.created", handler) }()

	time.Sleep(time.Second)
}
