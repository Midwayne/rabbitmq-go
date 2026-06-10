package rabbitmq

import (
	"testing"
	"time"

	"github.com/Midwayne/rabbitmq-go/pkg/rabbitmq/logging"
)

func TestConfigValidate(t *testing.T) {
	if err := (Config{}).validate(); err == nil {
		t.Error("empty config should fail validation")
	}
	if err := (Config{URL: "amqp://localhost"}).validate(); err == nil {
		t.Error("missing exchange should fail validation")
	}
	if err := (Config{URL: "amqp://localhost", Exchange: "events"}).validate(); err != nil {
		t.Errorf("valid config failed validation: %v", err)
	}
}

func TestConfigNormalizeDefaults(t *testing.T) {
	got := Config{URL: "amqp://localhost", Exchange: "events"}.normalize()

	checks := map[string]struct {
		got  any
		want any
	}{
		"ExchangeType":    {got.ExchangeType, defaultExchangeType},
		"ChannelPoolSize": {got.ChannelPoolSize, defaultChannelPoolSize},
		"MaxChannelAge":   {got.MaxChannelAge, time.Duration(defaultMaxChannelAge)},
		"PublishConfirmTimeout": {
			got.PublishConfirmTimeout,
			time.Duration(defaultPublishConfirmTimeout),
		},
		"DefaultContentType": {got.DefaultContentType, defaultContentType},
		"DialTimeout":        {got.DialTimeout, time.Duration(defaultDialTimeout)},
		"ConnectionTimeout": {
			got.ConnectionTimeout,
			time.Duration(defaultConnectionTimeout),
		},
		"ChannelCreateTimeout": {
			got.ChannelCreateTimeout,
			time.Duration(defaultChannelCreateTimeout),
		},
		"ConfirmEnableTimeout": {
			got.ConfirmEnableTimeout,
			time.Duration(defaultConfirmEnableTimeout),
		},
		"ReconnectMaxBackoff": {
			got.ReconnectMaxBackoff,
			time.Duration(defaultReconnectMaxBackoff),
		},
		"DeadLetterExchangeSuffix": {got.DeadLetterExchangeSuffix, defaultDLXSuffix},
		"DeadLetterQueueSuffix":    {got.DeadLetterQueueSuffix, defaultDLQSuffix},
	}
	for field, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", field, c.got, c.want)
		}
	}
}

func TestConfigNormalizePreservesOverrides(t *testing.T) {
	in := Config{
		URL:             "amqp://localhost",
		Exchange:        "events",
		ExchangeType:    "topic",
		ChannelPoolSize: 3,
		MaxChannelAge:   time.Minute,
	}
	got := in.normalize()

	if got.ExchangeType != "topic" {
		t.Errorf("ExchangeType = %q, want topic", got.ExchangeType)
	}
	if got.ChannelPoolSize != 3 {
		t.Errorf("ChannelPoolSize = %d, want 3", got.ChannelPoolSize)
	}
	if got.MaxChannelAge != time.Minute {
		t.Errorf("MaxChannelAge = %v, want 1m", got.MaxChannelAge)
	}
}

func TestConfigResolvers(t *testing.T) {
	cfg := Config{}
	if _, ok := cfg.logger().(logging.NopLogger); !ok {
		t.Error("logger() should default to logging.NopLogger")
	}
	if _, ok := cfg.instrumentation().(NopInstrumentation); !ok {
		t.Error("instrumentation() should default to NopInstrumentation")
	}
}

func TestConfigResolversReturnConfigured(t *testing.T) {
	customLogger := logging.NewSlogLogger(nil)
	customInstr := NopInstrumentation{}
	cfg := Config{Logger: customLogger, Instrumentation: customInstr}

	if cfg.logger() != customLogger {
		t.Error("logger() should return the configured logger")
	}
	if cfg.instrumentation() != customInstr {
		t.Error("instrumentation() should return the configured instrumentation")
	}
}
