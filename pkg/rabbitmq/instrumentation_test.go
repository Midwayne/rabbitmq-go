package rabbitmq

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNopInstrumentationImplementsInterface(t *testing.T) {
	var _ Instrumentation = NopInstrumentation{}
}

func TestNopInstrumentationStartPublish(t *testing.T) {
	ctx := context.Background()
	gotCtx, end := NopInstrumentation{}.StartPublish(ctx, &PublishContext{Headers: amqp.Table{}})
	if gotCtx != ctx {
		t.Error("StartPublish should return the supplied context unchanged")
	}
	end(nil)
	end(errors.New("boom")) // must not panic
}

func TestNopInstrumentationStartConsume(t *testing.T) {
	ctx := context.Background()
	gotCtx, end := NopInstrumentation{}.StartConsume(ctx, &DeliveryContext{Headers: amqp.Table{}})
	if gotCtx != ctx {
		t.Error("StartConsume should return the supplied context unchanged")
	}
	end(ConsumeResult{})
	end(
		ConsumeResult{Err: errors.New("boom"), DeadLettered: true, NonRetryable: true},
	) // must not panic
}
