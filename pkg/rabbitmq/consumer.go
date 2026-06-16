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

// Handler processes a single message. Returning nil acks the message. Returning
// an error triggers a bounded in-place retry; wrap the error with
// NewNonRetryableError to skip retries and dead-letter immediately.
type Handler func(ctx context.Context, msg Message) error

// MessageHandler processes a single message body. Prefer Handler for new code
// that needs message metadata.
type MessageHandler func(ctx context.Context, body []byte) error

// Message is the safe delivery view passed to handlers. Ack/nack methods are
// intentionally not exposed; the library owns acknowledgement semantics.
type Message struct {
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
	RoutingKey      string
	Exchange        string
	Redelivered     bool
	RetryCount      int
}

// Consumer consumes messages from queues bound to an exchange. Each consumed
// queue is declared with a dead-letter exchange/queue (unless disabled), and
// failed deliveries are retried up to Config.MaxRetries before being
// dead-lettered. The connection is re-established automatically on loss.
// A Consumer is safe for concurrent use.
type Consumer struct {
	cfg        Config
	log        logging.Logger
	instr      Instrumentation
	instrIsNop bool

	connection   *amqp.Connection
	connMu       sync.RWMutex
	reconnecting atomic.Bool
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	retryPublish func(*amqp.Channel, string, amqp.Delivery, int) error
}

// NewConsumer connects to the broker and declares the exchange (unless
// SkipExchangeDeclare is set). The supplied context governs the consumer's
// lifetime: cancelling it, or calling Close, stops all running consumers.
func NewConsumer(ctx context.Context, cfg Config) (*Consumer, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.normalize()

	clientCtx, cancel := context.WithCancel(ctx)
	instr := cfg.instrumentation()
	c := &Consumer{
		cfg:        cfg,
		log:        cfg.logger(),
		instr:      instr,
		instrIsNop: isNopInstrumentation(instr),
		ctx:        clientCtx,
		cancel:     cancel,
	}

	if err := c.connect(); err != nil {
		cancel()
		return nil, err
	}

	go c.watchConnection()

	return c, nil
}

// connect dials the broker and declares the exchange.
func (c *Consumer) connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	masked := amqpx.MaskURL(c.cfg.URL)
	conn, err := amqpx.Dial(amqpx.DialOptions{
		URL:               c.cfg.URL,
		TLSClientConfig:   c.cfg.TLSClientConfig,
		Heartbeat:         c.cfg.Heartbeat,
		Locale:            c.cfg.Locale,
		ClientProperties:  c.cfg.ClientProperties,
		ConnectionName:    c.cfg.ConnectionName,
		SASL:              c.cfg.SASL,
		DialTimeout:       c.cfg.DialTimeout,
		ConnectionTimeout: c.cfg.ConnectionTimeout,
	})
	if err != nil {
		c.log.Error(
			c.ctx,
			"failed to connect to RabbitMQ",
			logging.Err(err),
			logging.String("url", masked),
		)
		return err
	}
	c.connection = conn

	if !c.cfg.SkipExchangeDeclare {
		if err := amqpx.DeclareExchange(conn, c.cfg.Exchange, c.cfg.ExchangeType); err != nil {
			c.log.Error(
				c.ctx,
				"failed to declare exchange",
				logging.Err(err),
				logging.String("exchange", c.cfg.Exchange),
			)
			_ = conn.Close()
			return err
		}
	}

	c.log.Info(c.ctx, "connected to RabbitMQ", logging.String("exchange", c.cfg.Exchange))
	return nil
}

// watchConnection reconnects on unexpected connection loss.
func (c *Consumer) watchConnection() {
	for {
		c.connMu.RLock()
		conn := c.connection
		c.connMu.RUnlock()

		if conn == nil {
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(reconnectIdleWait):
				continue
			}
		}

		select {
		case <-c.ctx.Done():
			return
		case closeErr := <-conn.NotifyClose(make(chan *amqp.Error, 1)):
			if closeErr != nil {
				c.log.Error(c.ctx, "RabbitMQ connection closed unexpectedly", logging.Err(closeErr))
				c.reconnect()
			} else {
				c.log.Info(c.ctx, "RabbitMQ connection closed gracefully")
				return
			}
		}
	}
}

// reconnect re-establishes the connection with exponential backoff.
func (c *Consumer) reconnect() {
	if !c.reconnecting.CompareAndSwap(false, true) {
		return
	}
	defer c.reconnecting.Store(false)

	backoff := initialReconnectBackoff
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		c.log.Info(
			c.ctx,
			"attempting to reconnect to RabbitMQ",
			logging.Duration("backoff", backoff),
		)
		if err := sleepContext(c.ctx, backoff); err != nil {
			return
		}

		if err := c.connect(); err == nil {
			c.log.Info(c.ctx, "successfully reconnected to RabbitMQ")
			return
		}

		backoff *= 2
		if backoff > c.cfg.ReconnectMaxBackoff {
			backoff = c.cfg.ReconnectMaxBackoff
		}
	}
}

// Check reports whether the underlying connection is currently usable. It is
// suitable for wiring into a health endpoint.
func (c *Consumer) Check(context.Context) error {
	if c == nil {
		return errors.New("rabbitmq: consumer is nil")
	}

	c.connMu.RLock()
	defer c.connMu.RUnlock()

	if c.connection == nil {
		return errors.New("rabbitmq: connection is nil")
	}
	if c.connection.IsClosed() {
		return errors.New("rabbitmq: connection is closed")
	}
	return nil
}

// Consume declares the topology for queueName bound to routingKey and delivers
// every message to handler. It blocks until the consumer's context is
// cancelled (or Close is called), restarting the consumer session on transient
// errors. Call it in its own goroutine, once per queue.
func (c *Consumer) Consume(queueName, routingKey string, handler Handler) error {
	if handler == nil {
		return errors.New("rabbitmq: nil message handler")
	}
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}

		err := c.consumeOnce(queueName, routingKey, handler)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			c.log.Error(
				c.ctx,
				"consumer error, restarting",
				logging.Err(err),
				logging.String("queue", queueName),
			)
			if err := sleepContext(c.ctx, consumerRestartSleep); err != nil {
				return nil
			}
		}
	}
}

// ConsumeBody adapts a body-only handler for simple consumers. Prefer Consume
// for new code that needs message metadata.
func (c *Consumer) ConsumeBody(queueName, routingKey string, handler MessageHandler) error {
	if handler == nil {
		return errors.New("rabbitmq: nil message handler")
	}
	return c.Consume(queueName, routingKey, func(ctx context.Context, msg Message) error {
		return handler(ctx, msg.Body)
	})
}

// ConsumeConcurrent behaves like Consume but dispatches up to workers deliveries
// to handler concurrently, instead of one-at-a-time. Use it when a handler must
// block across deliveries (e.g. to accumulate a batch) without starving the
// consume loop — with the sequential Consume, a blocking handler would prevent
// any further delivery from arriving. Prefetch is raised to at least workers so
// the broker keeps the pool fed. Ack/nack/retry semantics are per-delivery and
// identical to Consume (the amqp channel serialises sends, and retries use
// per-publish deferred confirms, so concurrent handling is safe). workers <= 1
// falls back to Consume. Call it in its own goroutine, once per queue.
func (c *Consumer) ConsumeConcurrent(
	queueName, routingKey string,
	workers int,
	handler Handler,
) error {
	if handler == nil {
		return errors.New("rabbitmq: nil message handler")
	}
	if workers <= 1 {
		return c.Consume(queueName, routingKey, handler)
	}
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}

		err := c.consumeOnceConcurrent(queueName, routingKey, workers, handler)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			c.log.Error(
				c.ctx,
				"consumer error, restarting",
				logging.Err(err),
				logging.String("queue", queueName),
			)
			if err := sleepContext(c.ctx, consumerRestartSleep); err != nil {
				return nil
			}
		}
	}
}

// ConsumeBodyConcurrent is the body-only adapter for ConsumeConcurrent.
func (c *Consumer) ConsumeBodyConcurrent(
	queueName, routingKey string,
	workers int,
	handler MessageHandler,
) error {
	if handler == nil {
		return errors.New("rabbitmq: nil message handler")
	}
	return c.ConsumeConcurrent(
		queueName,
		routingKey,
		workers,
		func(ctx context.Context, msg Message) error {
			return handler(ctx, msg.Body)
		},
	)
}

// consumeOnce runs a single consumer session until the channel or connection
// drops.
func (c *Consumer) consumeOnce(queueName, routingKey string, handler Handler) error {
	c.connMu.RLock()
	conn := c.connection
	c.connMu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return errors.New("rabbitmq: connection not available")
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Qos(c.cfg.PrefetchCount, 0, false); err != nil {
		return err
	}
	if c.cfg.MaxRetries > 0 {
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("enable confirms for retry publisher: %w", err)
		}
	}

	msgs, chanClose, err := c.declareConsumerTopology(ch, queueName, routingKey)
	if err != nil {
		return err
	}

	c.log.Info(
		c.ctx,
		"started consuming",
		logging.String("queue", queueName),
		logging.String("routingKey", routingKey),
	)

	for {
		select {
		case <-c.ctx.Done():
			return context.Canceled
		case closeErr := <-chanClose:
			if closeErr != nil {
				return closeErr
			}
			return errors.New("rabbitmq: channel closed")
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("rabbitmq: message channel closed")
			}
			c.handleDelivery(ch, queueName, routingKey, msg, handler)
		}
	}
}

// consumeOnceConcurrent is consumeOnce with a bounded worker pool: each delivery
// is handled in its own goroutine (up to workers at a time). In-flight handlers
// are drained before the channel closes, so an in-progress ack never races the
// channel teardown.
func (c *Consumer) consumeOnceConcurrent(
	queueName, routingKey string,
	workers int,
	handler Handler,
) error {
	c.connMu.RLock()
	conn := c.connection
	c.connMu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return errors.New("rabbitmq: connection not available")
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	prefetch := max(c.cfg.PrefetchCount, workers)
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	if c.cfg.MaxRetries > 0 {
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("enable confirms for retry publisher: %w", err)
		}
	}

	msgs, chanClose, err := c.declareConsumerTopology(ch, queueName, routingKey)
	if err != nil {
		return err
	}

	c.log.Info(
		c.ctx,
		"started consuming (concurrent)",
		logging.String("queue", queueName),
		logging.String("routingKey", routingKey),
		logging.Int("workers", workers),
	)

	sem := make(chan struct{}, workers)
	var inflight sync.WaitGroup
	// Runs before the deferred ch.Close (LIFO): wait for handlers to finish so
	// none acks on a torn-down channel.
	defer inflight.Wait()

	for {
		select {
		case <-c.ctx.Done():
			return context.Canceled
		case closeErr := <-chanClose:
			if closeErr != nil {
				return closeErr
			}
			return errors.New("rabbitmq: channel closed")
		case msg, ok := <-msgs:
			if !ok {
				return errors.New("rabbitmq: message channel closed")
			}
			select {
			case sem <- struct{}{}:
			case <-c.ctx.Done():
				return context.Canceled
			case closeErr := <-chanClose:
				if closeErr != nil {
					return closeErr
				}
				return errors.New("rabbitmq: channel closed")
			}
			inflight.Add(1)
			go func(delivery amqp.Delivery) {
				defer inflight.Done()
				defer func() { <-sem }()
				c.handleDelivery(ch, queueName, routingKey, delivery, handler)
			}(msg)
		}
	}
}

// handleDelivery instruments, processes, and acks/nacks a single delivery.
func (c *Consumer) handleDelivery(
	ch *amqp.Channel,
	queueName, routingKey string,
	msg amqp.Delivery,
	handler Handler,
) {
	retryCount := amqpx.ReadRetryCount(msg.Headers)
	deliveryRoutingKey := msg.RoutingKey
	if deliveryRoutingKey == "" {
		deliveryRoutingKey = routingKey
	}
	ctx := c.ctx
	var end func(ConsumeResult)
	if !c.instrIsNop {
		delivery := &DeliveryContext{
			Queue:      queueName,
			RoutingKey: deliveryRoutingKey,
			BodySize:   len(msg.Body),
			RetryCount: retryCount,
			Headers:    msg.Headers,
		}
		ctx, end = c.instr.StartConsume(c.ctx, delivery)
	}

	var result ConsumeResult
	if handlerErr := c.callHandler(ctx, msg, retryCount, handler); handlerErr != nil {
		result = c.handleHandlerError(
			ctx,
			ch,
			queueName,
			deliveryRoutingKey,
			msg,
			retryCount,
			handlerErr,
		)
	} else {
		_ = msg.Ack(false)
		result.Acked = true
	}
	if end != nil {
		end(result)
	}
}

func (c *Consumer) callHandler(
	ctx context.Context,
	msg amqp.Delivery,
	retryCount int,
	handler Handler,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("rabbitmq: message handler panic: %v", recovered)
			c.log.Error(ctx, "message handler panic recovered", logging.Any("panic", recovered))
		}
	}()
	return handler(ctx, messageFromDelivery(msg, retryCount))
}

func messageFromDelivery(msg amqp.Delivery, retryCount int) Message {
	return Message{
		Body:            msg.Body,
		Headers:         msg.Headers,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    msg.DeliveryMode,
		Priority:        msg.Priority,
		CorrelationID:   msg.CorrelationId,
		ReplyTo:         msg.ReplyTo,
		Expiration:      msg.Expiration,
		MessageID:       msg.MessageId,
		Timestamp:       msg.Timestamp,
		Type:            msg.Type,
		UserID:          msg.UserId,
		AppID:           msg.AppId,
		RoutingKey:      msg.RoutingKey,
		Exchange:        msg.Exchange,
		Redelivered:     msg.Redelivered,
		RetryCount:      retryCount,
	}
}

// handleHandlerError decides between retry and dead-lettering for a failed
// delivery.
func (c *Consumer) handleHandlerError(
	ctx context.Context,
	ch *amqp.Channel,
	queueName, routingKey string,
	msg amqp.Delivery,
	retryCount int,
	handlerErr error,
) ConsumeResult {
	outcome := ConsumeResult{Err: handlerErr, NonRetryable: IsNonRetryable(handlerErr)}
	retriesExhausted := retryCount >= c.cfg.MaxRetries

	c.log.Error(ctx, "message handler failed",
		logging.Err(handlerErr),
		logging.String("queue", queueName),
		logging.Int("retry_count", retryCount),
		logging.Int("max_retries", c.cfg.MaxRetries),
		logging.Bool("retryable", !outcome.NonRetryable),
	)

	if outcome.NonRetryable || retriesExhausted {
		if retriesExhausted && !outcome.NonRetryable {
			c.log.Error(ctx, "max retries exceeded, dead-lettering message",
				logging.String("queue", queueName),
				logging.Int("retry_count", retryCount),
			)
		}
		// Nack without requeue routes to the dead-letter exchange (when
		// configured); otherwise the broker drops the message.
		_ = msg.Nack(false, false)
		outcome.DeadLettered = true
		outcome.Nacked = true
		return outcome
	}

	if pubErr := c.publishRetry(ch, routingKey, msg, retryCount+1); pubErr != nil {
		c.log.Error(ctx, "failed to republish message for retry",
			logging.Err(pubErr),
			logging.String("queue", queueName),
			logging.Int("retry_count", retryCount),
		)
		_ = msg.Nack(false, true)
		outcome.Nacked = true
		outcome.Requeued = true
		return outcome
	}
	_ = msg.Ack(false)
	outcome.Acked = true
	outcome.Retried = true
	return outcome
}

func (c *Consumer) publishRetry(
	ch *amqp.Channel,
	routingKey string,
	original amqp.Delivery,
	newRetryCount int,
) error {
	if c.retryPublish != nil {
		return c.retryPublish(ch, routingKey, original, newRetryCount)
	}
	return c.republishWithRetry(ch, routingKey, original, newRetryCount)
}

// Close stops every running consumer, waits for them to finish, and closes the
// connection.
func (c *Consumer) Close() {
	c.cancel()
	c.wg.Wait()

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.connection != nil {
		_ = c.connection.Close()
		c.log.Info(c.ctx, "RabbitMQ consumer connection closed")
	}
}

// republishWithRetry re-publishes a copy of the message with an incremented
// retry counter so it is redelivered.
func (c *Consumer) republishWithRetry(
	ch *amqp.Channel,
	routingKey string,
	original amqp.Delivery,
	newRetryCount int,
) error {
	headers, err := amqpx.NextRetryHeaders(original.Headers, newRetryCount)
	if err != nil {
		return err
	}

	pub := amqp.Publishing{
		Headers:         headers,
		ContentType:     original.ContentType,
		ContentEncoding: original.ContentEncoding,
		DeliveryMode:    original.DeliveryMode,
		Priority:        original.Priority,
		CorrelationId:   original.CorrelationId,
		ReplyTo:         original.ReplyTo,
		Expiration:      original.Expiration,
		MessageId:       original.MessageId,
		Timestamp:       original.Timestamp,
		Type:            original.Type,
		UserId:          original.UserId,
		AppId:           original.AppId,
		Body:            original.Body,
	}
	if pub.DeliveryMode == 0 {
		pub.DeliveryMode = amqp.Persistent
	}

	dc, err := ch.PublishWithDeferredConfirmWithContext(
		c.ctx,
		c.cfg.Exchange,
		routingKey,
		false,
		false,
		pub,
	)
	if err != nil {
		return err
	}
	if dc == nil {
		return errors.New("rabbitmq: publisher confirms not enabled on retry channel")
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.cfg.PublishConfirmTimeout)
	defer cancel()
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("rabbitmq: retry publish was not acknowledged by broker")
	}
	return nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
