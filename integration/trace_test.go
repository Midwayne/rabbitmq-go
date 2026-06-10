package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq"
	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
)

// TestTraceContextPropagatesThroughBroker proves the full tracing pipeline via
// the otelrabbitmq instrumentation: the publisher injects the active trace
// context into the message headers, the headers survive the round trip through
// RabbitMQ, and the consumer extracts them so its handler runs under a span
// that shares the original trace ID.
func TestTraceContextPropagatesThroughBroker(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	instr := otelrabbitmq.New(
		otelrabbitmq.WithTracerProvider(tp),
		otelrabbitmq.WithPropagator(propagation.TraceContext{}),
	)

	gotTraceID := make(chan trace.TraceID, 1)
	handler := func(ctx context.Context, _ rabbitmq.Message) error {
		gotTraceID <- trace.SpanContextFromContext(ctx).TraceID()
		return nil
	}

	startConsumer(t, url, rabbitmq.Config{Instrumentation: instr}, topo, handler)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:             url,
		Exchange:        topo.exchange,
		Instrumentation: instr,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ctx, parent := tp.Tracer("test").Start(context.Background(), "publish-parent")
	wantTraceID := parent.SpanContext().TraceID()

	if err := pub.Publish(ctx, topo.exchange, topo.routingKey, []byte("{}"), true); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	parent.End()

	select {
	case got := <-gotTraceID:
		if got != wantTraceID {
			t.Errorf("trace ID in handler = %s, want %s", got, wantTraceID)
		}
	case <-time.After(defaultTimeout):
		t.Fatal("handler was not invoked within the timeout")
	}
}

func TestTraceContextSurvivesRetry(t *testing.T) {
	url := brokerURL(t)
	conn := adminConn(t, url)
	topo := newTopology(t, conn)

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	instr := otelrabbitmq.New(
		otelrabbitmq.WithTracerProvider(tp),
		otelrabbitmq.WithPropagator(propagation.TraceContext{}),
	)

	gotTraceID := make(chan trace.TraceID, 1)
	var attempts atomic.Int32
	handler := func(ctx context.Context, _ rabbitmq.Message) error {
		if attempts.Add(1) == 1 {
			return errors.New("retry me")
		}
		gotTraceID <- trace.SpanContextFromContext(ctx).TraceID()
		return nil
	}
	startConsumer(t, url, rabbitmq.Config{Instrumentation: instr, MaxRetries: 1}, topo, handler)

	pub, err := rabbitmq.NewPublisher(rabbitmq.Config{
		URL:             url,
		Exchange:        topo.exchange,
		Instrumentation: instr,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer pub.Close()

	ctx, parent := tp.Tracer("test").Start(context.Background(), "publish-parent")
	wantTraceID := parent.SpanContext().TraceID()
	if err := pub.Publish(ctx, topo.exchange, topo.routingKey, []byte("{}"), true); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	parent.End()

	select {
	case got := <-gotTraceID:
		if got != wantTraceID {
			t.Errorf("trace ID in retry handler = %s, want %s", got, wantTraceID)
		}
	case <-time.After(defaultTimeout):
		t.Fatal("retry handler was not invoked within the timeout")
	}
}
