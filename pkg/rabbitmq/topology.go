package rabbitmq

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// declareConsumerTopology declares the (optional) dead-letter exchange/queue,
// the main queue, binds them, and starts a consumer. It returns the delivery
// stream and the channel-close notification.
func (c *Consumer) declareConsumerTopology(
	ch *amqp.Channel,
	queueName, routingKey string,
) (<-chan amqp.Delivery, <-chan *amqp.Error, error) {
	var queueArgs amqp.Table

	if !c.cfg.DisableDeadLetter {
		dlxName := c.cfg.Exchange + c.cfg.DeadLetterExchangeSuffix
		if err := ch.ExchangeDeclare(
			dlxName,
			deadLetterExchangeType,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return nil, nil, fmt.Errorf("declare dead-letter exchange: %w", err)
		}

		dlqName := queueName + c.cfg.DeadLetterQueueSuffix
		if _, err := ch.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
			return nil, nil, fmt.Errorf("declare dead-letter queue: %w", err)
		}
		if err := ch.QueueBind(dlqName, routingKey, dlxName, false, nil); err != nil {
			return nil, nil, fmt.Errorf("bind dead-letter queue: %w", err)
		}

		queueArgs = amqp.Table{
			"x-dead-letter-exchange":    dlxName,
			"x-dead-letter-routing-key": routingKey,
		}
	}

	q, err := ch.QueueDeclare(queueName, true, false, false, false, queueArgs)
	if err != nil {
		return nil, nil, wrapQueueDeclareErr(queueName, err)
	}

	if err := ch.QueueBind(q.Name, routingKey, c.cfg.Exchange, false, nil); err != nil {
		return nil, nil, fmt.Errorf("bind queue: %w", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
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
