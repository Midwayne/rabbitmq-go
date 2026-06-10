package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Midwayne/rabbitmq-go/internal/amqpx"
	"github.com/Midwayne/rabbitmq-go/pkg/rabbitmq/logging"
)

// Publisher publishes messages to an exchange. It maintains a single
// connection, an idle cache of confirm-enabled channels, and reconnects
// automatically with exponential backoff. It is safe for concurrent use.
type Publisher interface {
	// Publish sends body with default message properties and no mandatory routing
	// requirement. It is a convenience wrapper around PublishMessage. When
	// waitForConfirm is true it blocks until the broker acknowledges accepting the
	// publish; that does not guarantee the message was routed to a queue unless
	// mandatory publishing and returned-message handling are used.
	Publish(
		ctx context.Context,
		exchange, routingKey string,
		body []byte,
		waitForConfirm bool,
	) error
	// PublishMessage sends msg with explicit AMQP properties/options. When
	// msg.Exchange is empty, Config.Exchange is used. The configured
	// Instrumentation may mutate msg.Headers to inject propagation data.
	PublishMessage(ctx context.Context, msg PublishMessage) error
	// GetChannel borrows a confirm-enabled channel from the pool for advanced
	// use. This API is intentionally low-level: callers must not use the channel
	// concurrently, must preserve confirm mode invariants, and must return it with
	// ReturnChannel exactly once. Prefer Publish or PublishMessage unless you need
	// raw amqp091-go behavior.
	GetChannel() (*amqp.Channel, error)
	// ReturnChannel returns a channel borrowed via GetChannel to the pool.
	ReturnChannel(ch *amqp.Channel)
	// Close gracefully drains the pool and closes the connection.
	Close()
}

type publisher struct {
	cfg   Config
	log   logging.Logger
	instr Instrumentation

	connection   *amqp.Connection
	channelPool  chan *pooledChannel
	channelMeta  sync.Map
	reconnecting atomic.Bool
	connMu       sync.RWMutex
	poolMu       sync.RWMutex
	closed       atomic.Bool
	closeChan    chan struct{}
}

// PublishMessage describes a message and publish options.
type PublishMessage struct {
	Exchange        string
	RoutingKey      string
	Body            []byte
	Headers         amqp.Table
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       time.Time
	Type            string
	UserID          string
	AppID           string
	Mandatory       bool
	Immediate       bool
	WaitForConfirm  bool
}

// ReturnedMessage reports a mandatory publish that the broker accepted but
// could not route to any queue.
type ReturnedMessage struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
	Message    amqp.Publishing
}

// PublishReturnedError is returned by PublishMessage when Mandatory is true and
// RabbitMQ returns the publish as unroutable.
type PublishReturnedError struct {
	Returned ReturnedMessage
}

func (e *PublishReturnedError) Error() string {
	return fmt.Sprintf(
		"rabbitmq: mandatory publish returned unroutable: %d %s (exchange %q, routing key %q)",
		e.Returned.ReplyCode,
		e.Returned.ReplyText,
		e.Returned.Exchange,
		e.Returned.RoutingKey,
	)
}

// NewPublisher connects to the broker, declares the exchange (unless
// SkipExchangeDeclare is set), warms the channel pool, and starts the
// reconnection watcher.
func NewPublisher(cfg Config) (Publisher, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.normalize()

	p := &publisher{
		cfg:       cfg,
		log:       cfg.logger(),
		instr:     cfg.instrumentation(),
		closeChan: make(chan struct{}),
	}

	if err := p.connect(); err != nil {
		return nil, err
	}

	go p.watchConnection()

	return p, nil
}

// connect establishes the connection and pre-warms the channel pool.
func (p *publisher) connect() error {
	if err := p.establishConnection(); err != nil {
		return err
	}
	// Pre-warm the pool OUTSIDE the lock to avoid deadlock.
	p.warmChannelPool()
	return nil
}

// establishConnection dials the broker and declares the exchange under the lock.
func (p *publisher) establishConnection() error {
	p.connMu.Lock()
	defer p.connMu.Unlock()

	masked := amqpx.MaskURL(p.cfg.URL)
	p.log.Info(context.Background(), "connecting to RabbitMQ", logging.String("url", masked))

	conn, err := amqpx.Dial(amqpx.DialOptions{
		URL:               p.cfg.URL,
		TLSClientConfig:   p.cfg.TLSClientConfig,
		Heartbeat:         p.cfg.Heartbeat,
		Locale:            p.cfg.Locale,
		ClientProperties:  p.cfg.ClientProperties,
		ConnectionName:    p.cfg.ConnectionName,
		SASL:              p.cfg.SASL,
		DialTimeout:       p.cfg.DialTimeout,
		ConnectionTimeout: p.cfg.ConnectionTimeout,
	})
	if err != nil {
		p.log.Error(
			context.Background(),
			"failed to connect to RabbitMQ",
			logging.Err(err),
			logging.String("url", masked),
		)
		return err
	}
	p.connection = conn
	p.log.Info(context.Background(), "connected to RabbitMQ", logging.String("url", masked))

	if !p.cfg.SkipExchangeDeclare {
		if err := amqpx.DeclareExchange(conn, p.cfg.Exchange, p.cfg.ExchangeType); err != nil {
			p.log.Error(
				context.Background(),
				"failed to declare exchange",
				logging.Err(err),
				logging.String("exchange", p.cfg.Exchange),
			)
			_ = conn.Close()
			return err
		}
	}

	p.poolMu.Lock()
	oldPool := p.channelPool
	p.channelPool = make(chan *pooledChannel, p.cfg.ChannelPoolSize)
	p.poolMu.Unlock()
	p.drainPool(oldPool)
	return nil
}

// Publish implements Publisher.
func (p *publisher) Publish(
	ctx context.Context,
	exchange, routingKey string,
	body []byte,
	waitForConfirm bool,
) error {
	return p.PublishMessage(ctx, PublishMessage{
		Exchange:       exchange,
		RoutingKey:     routingKey,
		Body:           body,
		WaitForConfirm: waitForConfirm,
	})
}

// PublishMessage implements Publisher.
func (p *publisher) PublishMessage(ctx context.Context, msg PublishMessage) error {
	if p.closed.Load() {
		return errors.New("rabbitmq: publisher is closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	msg = p.normalizePublishMessage(msg)
	pubCtx := &PublishContext{
		Exchange:   msg.Exchange,
		RoutingKey: msg.RoutingKey,
		BodySize:   len(msg.Body),
		Headers:    msg.Headers,
	}
	ctx, end := p.instr.StartPublish(ctx, pubCtx)

	err := p.publish(ctx, msg)
	end(err)
	return err
}

func (p *publisher) normalizePublishMessage(msg PublishMessage) PublishMessage {
	if msg.Exchange == "" {
		msg.Exchange = p.cfg.Exchange
	}
	if msg.Headers == nil {
		msg.Headers = amqp.Table{}
	}
	if msg.ContentType == "" {
		msg.ContentType = p.cfg.DefaultContentType
	}
	if msg.DeliveryMode == 0 {
		msg.DeliveryMode = amqp.Persistent
	}
	return msg
}

func (p *publisher) publish(
	ctx context.Context,
	msg PublishMessage,
) error {
	ch, err := p.GetChannel()
	if err != nil {
		return err
	}
	returnToPool := true
	defer func() {
		if returnToPool {
			p.ReturnChannel(ch)
			return
		}
		_ = ch.Close()
		p.channelMeta.Delete(ch)
	}()

	pub := amqp.Publishing{
		Headers:         msg.Headers,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationID,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageId:       msg.MessageID,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserId:          msg.UserID,
		AppId:           msg.AppID,
		Body:            msg.Body,
	}

	if msg.Mandatory {
		// NotifyReturn listeners cannot be unregistered, so this channel must be
		// closed rather than pooled: a stale listener on a reused channel would
		// swallow (and eventually block) returns meant for later publishes.
		returnToPool = false
		returns := ch.NotifyReturn(make(chan amqp.Return, 1))
		if err := p.publishOnChannel(ctx, ch, msg, pub); err != nil {
			return err
		}
		select {
		case ret := <-returns:
			return &PublishReturnedError{Returned: returnedMessage(ret)}
		case <-time.After(mandatoryReturnWait):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return p.publishOnChannel(ctx, ch, msg, pub)
}

// returnedMessage converts a broker return into the exported ReturnedMessage.
func returnedMessage(ret amqp.Return) ReturnedMessage {
	return ReturnedMessage{
		ReplyCode:  ret.ReplyCode,
		ReplyText:  ret.ReplyText,
		Exchange:   ret.Exchange,
		RoutingKey: ret.RoutingKey,
		Message: amqp.Publishing{
			Headers:         ret.Headers,
			ContentType:     ret.ContentType,
			ContentEncoding: ret.ContentEncoding,
			DeliveryMode:    ret.DeliveryMode,
			Priority:        ret.Priority,
			CorrelationId:   ret.CorrelationId,
			ReplyTo:         ret.ReplyTo,
			Expiration:      ret.Expiration,
			MessageId:       ret.MessageId,
			Timestamp:       ret.Timestamp,
			Type:            ret.Type,
			UserId:          ret.UserId,
			AppId:           ret.AppId,
			Body:            ret.Body,
		},
	}
}

func (p *publisher) publishOnChannel(
	ctx context.Context,
	ch *amqp.Channel,
	msg PublishMessage,
	pub amqp.Publishing,
) error {
	if !msg.WaitForConfirm {
		return ch.PublishWithContext(
			ctx,
			msg.Exchange,
			msg.RoutingKey,
			msg.Mandatory,
			msg.Immediate,
			pub,
		)
	}

	// Use DeferredConfirmation rather than NotifyPublish on pooled channels:
	// amqp091-go broadcasts every ack to all NotifyPublish listeners with a
	// blocking send, so registering a listener per publish grows unbounded and
	// stalls confirms.
	dc, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		msg.Exchange,
		msg.RoutingKey,
		msg.Mandatory,
		msg.Immediate,
		pub,
	)
	if err != nil {
		return err
	}
	if dc == nil {
		return errors.New("rabbitmq: publisher confirms not enabled on channel")
	}

	waitCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishConfirmTimeout)
	defer cancel()

	acked, err := dc.WaitContext(waitCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return errors.New("rabbitmq: timeout waiting for publish confirmation")
		}
		return err
	}
	if !acked {
		return errors.New("rabbitmq: message was not acknowledged by broker")
	}
	return nil
}

// Close implements Publisher.
func (p *publisher) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}

	close(p.closeChan)

	p.connMu.Lock()
	defer p.connMu.Unlock()

	p.drainChannelPool()

	if p.connection != nil {
		_ = p.connection.Close()
		p.log.Info(context.Background(), "RabbitMQ publisher connection closed")
	}
}

// watchConnection reconnects on unexpected connection loss.
func (p *publisher) watchConnection() {
	for {
		select {
		case <-p.closeChan:
			return
		default:
		}

		p.connMu.RLock()
		conn := p.connection
		p.connMu.RUnlock()

		if conn == nil {
			if p.sleepOrClosed(reconnectIdleWait) {
				return
			}
			continue
		}

		notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-p.closeChan:
			return
		case closeErr := <-notifyClose:
			if closeErr != nil {
				p.log.Error(
					context.Background(),
					"RabbitMQ connection closed unexpectedly",
					logging.Err(closeErr),
				)
				p.drainChannelPool()
			} else {
				p.log.Info(context.Background(), "RabbitMQ connection closed gracefully")
				return
			}
			p.reconnect()
		}
	}
}

// reconnect re-establishes the connection with exponential backoff.
func (p *publisher) reconnect() {
	if !p.reconnecting.CompareAndSwap(false, true) {
		return
	}
	defer p.reconnecting.Store(false)

	backoff := initialReconnectBackoff
	for {
		select {
		case <-p.closeChan:
			return
		default:
		}

		p.log.Info(
			context.Background(),
			"attempting to reconnect to RabbitMQ",
			logging.Duration("backoff", backoff),
		)
		if p.sleepOrClosed(backoff) {
			return
		}

		if err := p.connect(); err == nil {
			p.log.Info(context.Background(), "successfully reconnected to RabbitMQ")
			return
		}

		backoff *= 2
		if backoff > p.cfg.ReconnectMaxBackoff {
			backoff = p.cfg.ReconnectMaxBackoff
		}
	}
}

func (p *publisher) sleepOrClosed(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-p.closeChan:
		return true
	case <-timer.C:
		return false
	}
}
