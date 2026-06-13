package rabbitmq

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// declareQueueTopology declares the durable queue bound to routingKey on
// cfg.Exchange, preceded by the dead-letter exchange/queue unless
// cfg.DisableDeadLetter is set. It is the single source of truth for queue
// arguments: consumers and publishers both declare through it, so either side
// can declare first and the other redeclares without a precondition conflict.
func declareQueueTopology(ch *amqp.Channel, cfg Config, queueName, routingKey string) error {
	var queueArgs amqp.Table

	if !cfg.DisableDeadLetter {
		dlxName := cfg.Exchange + cfg.DeadLetterExchangeSuffix
		if err := ch.ExchangeDeclare(
			dlxName,
			deadLetterExchangeType,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("declare dead-letter exchange: %w", err)
		}

		dlqName := queueName + cfg.DeadLetterQueueSuffix
		if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare dead-letter queue: %w", err)
		}
		if err := ch.QueueBind(dlqName, routingKey, dlxName, false, nil); err != nil {
			return fmt.Errorf("bind dead-letter queue: %w", err)
		}

		queueArgs = amqp.Table{
			"x-dead-letter-exchange":    dlxName,
			"x-dead-letter-routing-key": routingKey,
		}
	}

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, queueArgs); err != nil {
		return wrapQueueDeclareErr(queueName, err)
	}

	if err := ch.QueueBind(queueName, routingKey, cfg.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}

	return nil
}

// declareConsumerTopology declares the queue topology and starts a consumer.
// It returns the delivery stream and the channel-close notification.
func (c *Consumer) declareConsumerTopology(
	ch *amqp.Channel,
	queueName, routingKey string,
) (<-chan amqp.Delivery, <-chan *amqp.Error, error) {
	if err := declareQueueTopology(ch, c.cfg, queueName, routingKey); err != nil {
		return nil, nil, err
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return nil, nil, err
	}

	chanClose := ch.NotifyClose(make(chan *amqp.Error, 1))
	return msgs, chanClose, nil
}

// wrapQueueDeclareErr adds a hint when a queue redeclare fails because the
// existing queue has different arguments.
func wrapQueueDeclareErr(queueName string, err error) error {
	var aerr *amqp.Error
	if errors.As(err, &aerr) && aerr.Code == amqp.PreconditionFailed {
		return fmt.Errorf(
			"declare queue %q: %w (queue already exists with different arguments; delete %q on the broker, then restart)",
			queueName,
			err,
			queueName,
		)
	}
	return fmt.Errorf("declare queue: %w", err)
}
