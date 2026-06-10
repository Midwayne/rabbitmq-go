package rabbitmq

import (
	"crypto/tls"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Midwayne/rabbitmq-go/pkg/rabbitmq/logging"
)

const (
	defaultExchangeType          = "direct"
	defaultChannelPoolSize       = 10
	defaultMaxChannelAge         = 30 * time.Minute
	defaultPublishConfirmTimeout = 5 * time.Second
	defaultContentType           = "application/json"
	defaultDialTimeout           = 10 * time.Second
	defaultConnectionTimeout     = 15 * time.Second
	defaultChannelCreateTimeout  = 5 * time.Second
	defaultConfirmEnableTimeout  = 5 * time.Second
	mandatoryReturnWait          = 25 * time.Millisecond
	defaultReconnectMaxBackoff   = 30 * time.Second
	defaultDLXSuffix             = ".dlx"
	defaultDLQSuffix             = ".dlq"

	initialReconnectBackoff = time.Second
	reconnectIdleWait       = 5 * time.Second
	consumerRestartSleep    = 5 * time.Second

	deadLetterExchangeType = "direct"
)

// Config configures a Publisher or a Consumer. Only URL and Exchange are
// required; every other field falls back to a sensible default. The same
// Config type builds both ends of a topology, with fields grouped by the role
// they apply to.
type Config struct {
	// URL is the AMQP connection string, e.g.
	// "amqp://guest:guest@localhost:5672/". Required.
	URL string

	// Exchange is the exchange published to (Publisher) or bound from
	// (Consumer). Required.
	Exchange string
	// ExchangeType is the AMQP exchange type ("direct", "topic", "fanout",
	// "headers"). Defaults to "direct".
	ExchangeType string
	// SkipExchangeDeclare disables declaring the exchange on connect. Set it
	// when the exchange is managed externally. The zero value declares the
	// exchange as durable.
	SkipExchangeDeclare bool

	// --- Publisher tuning ---

	// ChannelPoolSize is the maximum number of idle confirm-enabled publisher
	// channels kept for reuse. It is not a concurrency limit: when all idle
	// channels are borrowed, Publish may open another temporary channel.
	// Defaults to 10.
	ChannelPoolSize int
	// MaxChannelAge is how long a pooled channel may live before it is
	// recycled. Defaults to 30m.
	MaxChannelAge time.Duration
	// PublishConfirmTimeout bounds the wait for a broker ack when a publish
	// requests confirmation. Defaults to 5s.
	PublishConfirmTimeout time.Duration
	// DefaultContentType is the content type stamped on published messages.
	// Defaults to "application/json".
	DefaultContentType string

	// --- Consumer tuning ---

	// PrefetchCount is the consumer QoS prefetch. Defaults to 0 (no limit).
	// Unlimited prefetch can overwhelm a process; production consumers should set
	// this explicitly.
	PrefetchCount int
	// MaxRetries is the number of in-place redeliveries attempted before a
	// failing message is dead-lettered. Defaults to 0 (dead-letter on the
	// first failure). Messages wrapped in NonRetryableError skip retries.
	MaxRetries int
	// DisableDeadLetter turns off dead-letter exchange/queue declaration and
	// routing. Failed messages are then dropped once retries are exhausted.
	DisableDeadLetter bool
	// DeadLetterExchangeSuffix is appended to Exchange to form the dead-letter
	// exchange name. Defaults to ".dlx".
	DeadLetterExchangeSuffix string
	// DeadLetterQueueSuffix is appended to the consumed queue name to form the
	// dead-letter queue name. Defaults to ".dlq".
	DeadLetterQueueSuffix string

	// --- Connection / reconnection tuning ---

	// DialTimeout bounds the underlying TCP dial. Defaults to 10s.
	DialTimeout time.Duration
	// TLSClientConfig enables TLS, custom CAs, or mTLS for AMQP connections.
	TLSClientConfig *tls.Config
	// Heartbeat configures AMQP heartbeats. Leave zero for the amqp091-go default.
	Heartbeat time.Duration
	// Locale configures the AMQP locale. Leave empty for the amqp091-go default.
	Locale string
	// ClientProperties are sent during connection negotiation.
	ClientProperties amqp.Table
	// ConnectionName, when set, is sent as the RabbitMQ connection_name client
	// property. It is merged with ClientProperties.
	ConnectionName string
	// SASL configures authentication mechanisms. Leave nil for the amqp091-go
	// default plain auth derived from the URL.
	SASL []amqp.Authentication
	// ConnectionTimeout bounds the full AMQP handshake. Defaults to 15s.
	ConnectionTimeout time.Duration
	// ChannelCreateTimeout bounds opening a channel. Defaults to 5s.
	ChannelCreateTimeout time.Duration
	// ConfirmEnableTimeout bounds enabling publisher confirms on a channel.
	// Defaults to 5s.
	ConfirmEnableTimeout time.Duration
	// ReconnectMaxBackoff caps the exponential reconnect backoff. Defaults to
	// 30s.
	ReconnectMaxBackoff time.Duration

	// --- Observability ---

	// Logger receives structured logs. Defaults to logging.NopLogger.
	Logger logging.Logger
	// Instrumentation hooks tracing/metrics/propagation into publish and consume.
	// Defaults to NopInstrumentation (no observability, no extra dependencies).
	// For OpenTelemetry, set this to otelrabbitmq.New(...) from the
	// github.com/Midwayne/rabbitmq-go/pkg/otelrabbitmq module.
	Instrumentation Instrumentation
}

// validate checks the required fields.
func (c Config) validate() error {
	if c.URL == "" {
		return errors.New("rabbitmq: Config.URL is required")
	}
	if c.Exchange == "" {
		return errors.New("rabbitmq: Config.Exchange is required")
	}
	return nil
}

// normalize returns a copy of c with all unset fields replaced by defaults.
func (c Config) normalize() Config {
	if c.ExchangeType == "" {
		c.ExchangeType = defaultExchangeType
	}
	if c.ChannelPoolSize <= 0 {
		c.ChannelPoolSize = defaultChannelPoolSize
	}
	if c.MaxChannelAge <= 0 {
		c.MaxChannelAge = defaultMaxChannelAge
	}
	if c.PublishConfirmTimeout <= 0 {
		c.PublishConfirmTimeout = defaultPublishConfirmTimeout
	}
	if c.DefaultContentType == "" {
		c.DefaultContentType = defaultContentType
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.ConnectionTimeout <= 0 {
		c.ConnectionTimeout = defaultConnectionTimeout
	}
	if c.ChannelCreateTimeout <= 0 {
		c.ChannelCreateTimeout = defaultChannelCreateTimeout
	}
	if c.ConfirmEnableTimeout <= 0 {
		c.ConfirmEnableTimeout = defaultConfirmEnableTimeout
	}
	if c.ReconnectMaxBackoff <= 0 {
		c.ReconnectMaxBackoff = defaultReconnectMaxBackoff
	}
	if c.DeadLetterExchangeSuffix == "" {
		c.DeadLetterExchangeSuffix = defaultDLXSuffix
	}
	if c.DeadLetterQueueSuffix == "" {
		c.DeadLetterQueueSuffix = defaultDLQSuffix
	}
	return c
}

// logger resolves the configured Logger, falling back to logging.NopLogger.
func (c Config) logger() logging.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return logging.NopLogger{}
}

// instrumentation resolves the configured Instrumentation, falling back to
// NopInstrumentation.
func (c Config) instrumentation() Instrumentation {
	if c.Instrumentation != nil {
		return c.Instrumentation
	}
	return NopInstrumentation{}
}
