package amqpx

import (
	"fmt"
	"maps"
	"math"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RetryCountHeader is the message header tracking how many times a delivery has
// been retried in place.
const RetryCountHeader = "x-retry-count"

// ReadRetryCount reads the retry counter from message headers, returning 0 when
// it is absent or of an unexpected type.
func ReadRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	val, ok := headers[RetryCountHeader]
	if !ok {
		return 0
	}
	switch v := val.(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}

// NextRetryHeaders returns a copy of original with the retry counter set to
// count. It errors if count does not fit in the int32 AMQP header type.
func NextRetryHeaders(original amqp.Table, count int) (amqp.Table, error) {
	if count > math.MaxInt32 || count < math.MinInt32 {
		return nil, fmt.Errorf("rabbitmq: retry count out of int32 range: %d", count)
	}
	headers := make(amqp.Table, len(original)+1)
	maps.Copy(headers, original)
	headers[RetryCountHeader] = int32(count) //nolint:gosec // bounds checked above
	return headers, nil
}
