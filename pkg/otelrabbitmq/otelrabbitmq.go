// Package otelrabbitmq is an OpenTelemetry implementation of
// rabbitmq.Instrumentation. It creates producer and consumer spans, records
// message metrics, and propagates W3C trace context through message headers.
// It is side-effect-free: it does not initialize exporters, configure global
// OpenTelemetry state, or call otel.SetTracerProvider.
//
// It lives in its own module so the core github.com/Midwayne/rabbitmq-go
// library carries no OpenTelemetry dependency. Wire it in like so:
//
//	import (
//	    rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
//	    "github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq"
//	)
//
//	instr := otelrabbitmq.New(
//	    otelrabbitmq.WithTracerProvider(tp),
//	    otelrabbitmq.WithMeterProvider(mp),
//	    otelrabbitmq.WithPropagator(propagation.TraceContext{}),
//	)
//	cfg := rabbitmq.Config{Instrumentation: instr}
package otelrabbitmq

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	rabbitmq "github.com/Midwayne/rabbitmq-go/pkg/rabbitmq"
)

const (
	instrumentationName = "github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq"
	defaultMetricPrefix = "rabbitmq"
)

// Option configures the OpenTelemetry instrumentation.
type Option func(*config)

type config struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	propagator     propagation.TextMapPropagator
	metricPrefix   string
}

// WithTracerProvider sets the tracer provider. Defaults to
// otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) {
		if tp != nil {
			c.tracerProvider = tp
		}
	}
}

// WithMeterProvider sets the meter provider. Defaults to otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) {
		if mp != nil {
			c.meterProvider = mp
		}
	}
}

// WithPropagator sets the propagator used to move trace context through message
// headers. Defaults to otel.GetTextMapPropagator().
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) {
		if p != nil {
			c.propagator = p
		}
	}
}

// WithMetricPrefix namespaces the emitted metrics; prefix "rabbitmq" yields
// "rabbitmq.messages.total". Defaults to "rabbitmq".
func WithMetricPrefix(prefix string) Option {
	return func(c *config) {
		if prefix != "" {
			c.metricPrefix = prefix
		}
	}
}

// New returns a rabbitmq.Instrumentation backed by OpenTelemetry. With no
// options it uses the global tracer/meter providers and the global propagator.
func New(opts ...Option) rabbitmq.Instrumentation {
	cfg := config{
		tracerProvider: otel.GetTracerProvider(),
		meterProvider:  otel.GetMeterProvider(),
		propagator:     otel.GetTextMapPropagator(),
		metricPrefix:   defaultMetricPrefix,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &instrumentation{
		tracer:     cfg.tracerProvider.Tracer(instrumentationName),
		propagator: cfg.propagator,
		metrics:    newMetrics(cfg.meterProvider.Meter(instrumentationName), cfg.metricPrefix),
	}
}

type instrumentation struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	metrics    metrics
}

// StartPublish starts a producer span and injects trace context into the
// outgoing message headers.
func (in *instrumentation) StartPublish(
	ctx context.Context,
	p *rabbitmq.PublishContext,
) (context.Context, func(error)) {
	ctx, span := in.tracer.Start(
		ctx,
		"rabbitmq.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", p.Exchange),
			attribute.String("messaging.rabbitmq.routing_key", p.RoutingKey),
			attribute.Int("messaging.message.body.size", p.BodySize),
		),
	)
	in.propagator.Inject(ctx, headerCarrier(p.Headers))

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}
}

// StartConsume extracts trace context from the incoming headers and starts a
// consumer span, recording metrics when the delivery completes.
func (in *instrumentation) StartConsume(
	ctx context.Context,
	d *rabbitmq.DeliveryContext,
) (context.Context, func(rabbitmq.ConsumeResult)) {
	if len(d.Headers) > 0 {
		ctx = in.propagator.Extract(ctx, headerCarrier(d.Headers))
	}

	ctx, span := in.tracer.Start(
		ctx,
		"rabbitmq.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination.name", d.Queue),
			attribute.String("messaging.rabbitmq.routing_key", d.RoutingKey),
			attribute.String("messaging.operation", "process"),
			attribute.Int("messaging.message.body.size", d.BodySize),
			attribute.Int("messaging.retry_count", d.RetryCount),
		),
	)
	startedAt := time.Now()

	return ctx, func(res rabbitmq.ConsumeResult) {
		status := "ok"
		if res.Err != nil {
			status = "error"
			span.RecordError(res.Err)
			span.SetStatus(codes.Error, res.Err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}

		attrs := []attribute.KeyValue{
			attribute.String("messaging.destination.name", d.Queue),
			attribute.String("messaging.rabbitmq.routing_key", d.RoutingKey),
			attribute.String("messaging.process.status", status),
			attribute.Bool("messaging.rabbitmq.acked", res.Acked),
			attribute.Bool("messaging.rabbitmq.nacked", res.Nacked),
			attribute.Bool("messaging.rabbitmq.requeued", res.Requeued),
			attribute.Bool("messaging.rabbitmq.retried", res.Retried),
		}
		span.SetAttributes(attrs...)
		// Build the attribute set once so the counter and histogram do not each
		// sort and deduplicate the same attributes. NewSet may reorder attrs, so
		// it must come after span.SetAttributes.
		metricAttrs := metric.WithAttributeSet(attribute.NewSet(attrs...))
		in.metrics.messages.Add(ctx, 1, metricAttrs)
		in.metrics.duration.Record(
			ctx,
			time.Since(startedAt).Seconds(),
			metricAttrs,
		)
		if res.DeadLettered {
			in.metrics.deadLetter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("messaging.destination.name", d.Queue),
				attribute.Bool("messaging.non_retryable", res.NonRetryable),
			))
		}
		span.End()
	}
}
