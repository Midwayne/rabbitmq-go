package otelrabbitmq

import (
	"sort"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHeaderCarrierGet(t *testing.T) {
	carrier := headerCarrier(amqp.Table{
		"str":   "value",
		"bytes": []byte("raw"),
		"int":   int32(7),
	})

	if got := carrier.Get("str"); got != "value" {
		t.Errorf("Get(str) = %q, want %q", got, "value")
	}
	if got := carrier.Get("bytes"); got != "raw" {
		t.Errorf("Get(bytes) = %q, want %q", got, "raw")
	}
	if got := carrier.Get("int"); got != "7" {
		t.Errorf("Get(int) = %q, want %q", got, "7")
	}
	if got := carrier.Get("missing"); got != "" {
		t.Errorf("Get(missing) = %q, want empty", got)
	}
}

func TestHeaderCarrierSetAndKeys(t *testing.T) {
	carrier := headerCarrier(amqp.Table{})
	carrier.Set("traceparent", "abc")
	carrier.Set("tracestate", "def")

	if got := carrier.Get("traceparent"); got != "abc" {
		t.Errorf("Get(traceparent) = %q, want %q", got, "abc")
	}

	keys := carrier.Keys()
	sort.Strings(keys)
	want := []string{"traceparent", "tracestate"}
	if len(keys) != len(want) || keys[0] != want[0] || keys[1] != want[1] {
		t.Errorf("Keys() = %v, want %v", keys, want)
	}
}

func TestHeaderCarrierNilSafe(t *testing.T) {
	var carrier headerCarrier
	if got := carrier.Get("x"); got != "" {
		t.Errorf("nil carrier Get = %q, want empty", got)
	}
	carrier.Set("x", "y") // must not panic
	if keys := carrier.Keys(); len(keys) != 0 {
		t.Errorf("nil carrier Keys = %v, want empty", keys)
	}
}
