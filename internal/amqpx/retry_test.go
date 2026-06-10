package amqpx

import (
	"math"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestReadRetryCount(t *testing.T) {
	cases := []struct {
		name    string
		headers amqp.Table
		want    int
	}{
		{"nil headers", nil, 0},
		{"missing header", amqp.Table{}, 0},
		{"int32", amqp.Table{RetryCountHeader: int32(3)}, 3},
		{"int64", amqp.Table{RetryCountHeader: int64(4)}, 4},
		{"int", amqp.Table{RetryCountHeader: 5}, 5},
		{"unexpected type", amqp.Table{RetryCountHeader: "nope"}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ReadRetryCount(tc.headers); got != tc.want {
				t.Errorf("ReadRetryCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestNextRetryHeaders(t *testing.T) {
	original := amqp.Table{"foo": "bar", RetryCountHeader: int32(1)}

	got, err := NextRetryHeaders(original, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["foo"] != "bar" {
		t.Errorf("existing headers should be copied, got %v", got)
	}
	if got[RetryCountHeader] != int32(2) {
		t.Errorf("retry count = %v, want int32(2)", got[RetryCountHeader])
	}

	// The original map must not be mutated.
	if original[RetryCountHeader] != int32(1) {
		t.Errorf("original headers were mutated: %v", original)
	}

	if _, err := NextRetryHeaders(nil, math.MaxInt64); err == nil {
		t.Error("expected overflow error for out-of-range retry count")
	}
}

func TestNextRetryHeadersNilOriginal(t *testing.T) {
	got, err := NextRetryHeaders(nil, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[RetryCountHeader] != int32(1) {
		t.Errorf("retry count = %v, want int32(1)", got[RetryCountHeader])
	}
}
