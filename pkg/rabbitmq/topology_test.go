package rabbitmq

import (
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestWrapQueueDeclareErr(t *testing.T) {
	precondition := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "args differ"}
	wrapped := wrapQueueDeclareErr("events", precondition)
	if wrapped == nil {
		t.Fatal("expected wrapped error")
	}
	msg := wrapped.Error()
	if !strings.Contains(msg, "events") || !strings.Contains(msg, "delete") {
		t.Errorf("precondition error message missing hint: %q", msg)
	}

	generic := &amqp.Error{Code: amqp.InternalError, Reason: "boom"}
	if wrapQueueDeclareErr("events", generic) == nil {
		t.Error("expected wrapped generic error")
	}
}
