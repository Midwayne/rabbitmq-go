package otelrabbitmq_test

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq"
	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
)

// TestImplementsInterface asserts New satisfies the core interface. The
// end-to-end trace-propagation behaviour (using the OpenTelemetry SDK and a
// real broker) is verified in the integration module.
func TestImplementsInterface(t *testing.T) {
	assertInstrumentation(t, otelrabbitmq.New())
}

func assertInstrumentation(t *testing.T, _ rabbitmq.Instrumentation) {
	t.Helper()
}

// TestNewWithExplicitProviders wires explicit (noop) tracer and meter providers,
// a propagator, and a metric prefix, then drives both hooks so the configured
// providers are actually exercised.
func TestNewWithExplicitProviders(t *testing.T) {
	instr := otelrabbitmq.New(
		otelrabbitmq.WithTracerProvider(tracenoop.NewTracerProvider()),
		otelrabbitmq.WithMeterProvider(metricnoop.NewMeterProvider()),
		otelrabbitmq.WithPropagator(propagation.TraceContext{}),
		otelrabbitmq.WithMetricPrefix("custom"),
	)

	_, endPub := instr.StartPublish(context.Background(), &rabbitmq.PublishContext{
		Exchange: "ex", RoutingKey: "rk", BodySize: 1, Headers: amqp.Table{},
	})
	endPub(nil)

	_, endCons := instr.StartConsume(context.Background(), &rabbitmq.DeliveryContext{
		Queue: "q", RoutingKey: "rk", BodySize: 1, Headers: amqp.Table{},
	})
	endCons(rabbitmq.ConsumeResult{Acked: true})
}

// TestOptionsIgnoreNilArguments asserts the With* options keep their defaults
// when handed nil, rather than overwriting with a nil provider/propagator.
func TestOptionsIgnoreNilArguments(t *testing.T) {
	// Must not panic: nil options are ignored and the global defaults remain.
	instr := otelrabbitmq.New(
		otelrabbitmq.WithTracerProvider(nil),
		otelrabbitmq.WithMeterProvider(nil),
		otelrabbitmq.WithPropagator(nil),
		otelrabbitmq.WithMetricPrefix(""),
	)
	_, end := instr.StartPublish(
		context.Background(),
		&rabbitmq.PublishContext{Headers: amqp.Table{}},
	)
	end(nil)
}

// TestStartPublishLifecycle exercises the publish hook with the default
// (global, no-op) providers: it returns a usable context and a completion
// callback that is safe for both success and error outcomes.
func TestStartPublishLifecycle(t *testing.T) {
	instr := otelrabbitmq.New()

	headers := amqp.Table{}
	ctx, end := instr.StartPublish(context.Background(), &rabbitmq.PublishContext{
		Exchange: "ex", RoutingKey: "rk", BodySize: 3, Headers: headers,
	})
	if ctx == nil {
		t.Fatal("StartPublish returned a nil context")
	}
	end(nil)
	end(errors.New("boom"))
}

// TestStartConsumeLifecycle exercises the consume hook for every outcome,
// including a custom metric prefix and non-empty headers (extraction path).
func TestStartConsumeLifecycle(t *testing.T) {
	instr := otelrabbitmq.New(otelrabbitmq.WithMetricPrefix("custom"))

	outcomes := []rabbitmq.ConsumeResult{
		{},
		{Err: errors.New("boom")},
		{Err: errors.New("boom"), DeadLettered: true, NonRetryable: true},
	}
	for _, res := range outcomes {
		ctx, end := instr.StartConsume(context.Background(), &rabbitmq.DeliveryContext{
			Queue:      "q",
			RoutingKey: "rk",
			BodySize:   2,
			RetryCount: 1,
			Headers:    amqp.Table{"x": "y"},
		})
		if ctx == nil {
			t.Fatal("StartConsume returned a nil context")
		}
		end(res)
	}
}

func TestStartPublishInjectsTraceContext(t *testing.T) {
	instr := otelrabbitmq.New(otelrabbitmq.WithPropagator(propagation.TraceContext{}))
	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	ctx := trace.ContextWithSpanContext(
		context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
			Remote:     true,
		}),
	)
	headers := amqp.Table{}

	_, end := instr.StartPublish(ctx, &rabbitmq.PublishContext{
		Exchange: "ex", RoutingKey: "rk", BodySize: 1, Headers: headers,
	})
	defer end(nil)

	if got := headers["traceparent"]; got == nil {
		t.Fatalf("traceparent header missing after StartPublish: %#v", headers)
	}
}

func TestStartConsumeExtractsTraceContext(t *testing.T) {
	instr := otelrabbitmq.New(otelrabbitmq.WithPropagator(propagation.TraceContext{}))
	headers := amqp.Table{
		"traceparent": "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01",
	}

	ctx, end := instr.StartConsume(context.Background(), &rabbitmq.DeliveryContext{
		Queue: "q", RoutingKey: "rk", BodySize: 1, Headers: headers,
	})
	defer end(rabbitmq.ConsumeResult{})

	spanCtx := trace.SpanContextFromContext(ctx)
	if !spanCtx.IsValid() {
		t.Fatal("extracted span context is invalid")
	}
	if !spanCtx.IsRemote() {
		t.Fatal("extracted span context is not marked remote")
	}
}
