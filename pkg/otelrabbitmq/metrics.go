package otelrabbitmq

import "go.opentelemetry.io/otel/metric"

// metrics holds the instruments emitted while consuming. They are created from
// a meter with a configurable name prefix so several consumers can coexist.
type metrics struct {
	messages   metric.Int64Counter
	duration   metric.Float64Histogram
	deadLetter metric.Int64Counter
}

// newMetrics builds the instruments under the given prefix; e.g. prefix
// "rabbitmq" yields "rabbitmq.messages.total". Instrument creation errors are
// ignored because the OpenTelemetry API still returns usable no-op instruments.
func newMetrics(m metric.Meter, prefix string) metrics {
	messages, _ := m.Int64Counter(
		prefix+".messages.total",
		metric.WithDescription("Total RabbitMQ messages processed by the consumer."),
	)
	duration, _ := m.Float64Histogram(
		prefix+".message.duration.seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent processing a RabbitMQ message."),
	)
	deadLetter, _ := m.Int64Counter(
		prefix+".deadletter.total",
		metric.WithDescription("Total messages sent to the dead-letter queue."),
	)
	return metrics{messages: messages, duration: duration, deadLetter: deadLetter}
}
