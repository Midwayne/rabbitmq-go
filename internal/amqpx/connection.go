// Package amqpx holds the low-level AMQP plumbing shared by the publisher and
// consumer: connection dialing, exchange declaration, credential-safe URL
// masking, trace-context propagation, and the retry-count header. It is
// internal so the public API stays small and this layer can evolve freely.
package amqpx

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const schemeSep = "://"

// DialOptions configures Dial.
type DialOptions struct {
	URL string
	// TLSClientConfig enables TLS, custom CAs, or mTLS.
	TLSClientConfig *tls.Config
	// Heartbeat configures AMQP heartbeats. Leave zero for amqp091-go defaults.
	Heartbeat time.Duration
	// Locale configures the AMQP locale. Leave empty for amqp091-go defaults.
	Locale string
	// ClientProperties are sent during connection negotiation.
	ClientProperties amqp.Table
	// ConnectionName is sent as the RabbitMQ connection_name client property.
	ConnectionName string
	// SASL configures authentication mechanisms.
	SASL []amqp.Authentication
	// DialTimeout bounds the underlying TCP dial.
	DialTimeout time.Duration
	// ConnectionTimeout bounds the full AMQP handshake.
	ConnectionTimeout time.Duration
}

// Dial opens a new AMQP connection bounded by the dial and overall connection
// timeouts. The whole DialConfig call is raced against ConnectionTimeout so a
// stalled handshake cannot block startup indefinitely.
func Dial(opts DialOptions) (*amqp.Connection, error) {
	amqpCfg := amqp.Config{
		TLSClientConfig: opts.TLSClientConfig,
		Heartbeat:       opts.Heartbeat,
		Locale:          opts.Locale,
		SASL:            opts.SASL,
		Dial: func(network, addr string) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
			defer cancel()

			d := &net.Dialer{Timeout: opts.DialTimeout}
			return d.DialContext(ctx, network, addr)
		},
	}
	if opts.ClientProperties != nil {
		amqpCfg.Properties = opts.ClientProperties
	}
	if opts.ConnectionName != "" {
		if amqpCfg.Properties == nil {
			amqpCfg.Properties = amqp.Table{}
		}
		amqpCfg.Properties["connection_name"] = opts.ConnectionName
	}

	type result struct {
		conn *amqp.Connection
		err  error
	}
	resultChan := make(chan result, 1)

	go func() {
		conn, err := amqp.DialConfig(opts.URL, amqpCfg)
		select {
		case resultChan <- result{conn: conn, err: err}:
		default:
			// Timed out already; discard the late connection.
			if conn != nil {
				_ = conn.Close()
			}
		}
	}()

	select {
	case res := <-resultChan:
		return res.conn, res.err
	case <-time.After(opts.ConnectionTimeout):
		return nil, errors.New(
			"rabbitmq: connection timeout: broker may not be running or is unreachable",
		)
	}
}

// DeclareExchange declares a durable exchange of the given kind on a temporary
// channel.
func DeclareExchange(conn *amqp.Connection, name, kind string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	return ch.ExchangeDeclare(
		name,
		kind,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
}

// MaskURL replaces the password in an AMQP URL with "***" so the URL is safe to
// log. URLs without userinfo or without a password are returned unchanged.
func MaskURL(raw string) string {
	schemeIdx := strings.Index(raw, schemeSep)
	if schemeIdx < 0 {
		return raw
	}
	prefix := raw[:schemeIdx+len(schemeSep)]
	rest := raw[schemeIdx+len(schemeSep):]

	// The authority ends at the first path/query/fragment delimiter; the
	// password cannot legally contain one of these unencoded.
	authEnd := len(rest)
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		authEnd = i
	}

	// The last '@' within the authority separates userinfo from host, so a
	// password containing an (encoded or stray) '@' is handled correctly.
	atIdx := strings.LastIndex(rest[:authEnd], "@")
	if atIdx < 0 {
		return raw // no userinfo
	}

	username, _, hasPassword := strings.Cut(rest[:atIdx], ":")
	if !hasPassword {
		return raw // username only, no password
	}

	return prefix + username + ":***" + rest[atIdx:]
}
