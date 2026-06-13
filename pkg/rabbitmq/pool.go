package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Midwayne/rabbitmq-go/pkg/rabbitmq/logging"
)

// pooledChannel wraps a channel with its creation time for staleness checks.
type pooledChannel struct {
	ch        *amqp.Channel
	createdAt time.Time
}

// warmChannelPool fills the pool with channels. Must run OUTSIDE connMu.
func (p *publisher) warmChannelPool() {
	warmed := 0
	for attempt := range p.cfg.ChannelPoolSize {
		pch := p.createPooledChannel()
		if pch == nil {
			p.log.Warn(
				context.Background(),
				"failed to create channel for pool",
				logging.Int("attempt", attempt+1),
			)
			continue
		}
		// The send must not block while holding the read lock: concurrent
		// returns can fill the pool, and a stuck send would stall the write
		// lock taken on reconnect (and with it every publisher).
		p.poolMu.RLock()
		pool := p.channelPool
		stored := false
		if pool != nil {
			select {
			case pool <- pch:
				stored = true
			default:
			}
		}
		p.poolMu.RUnlock()
		if stored {
			warmed++
		} else {
			_ = pch.ch.Close()
		}
	}

	p.log.Info(context.Background(), "RabbitMQ publisher ready",
		logging.String("exchange", p.cfg.Exchange),
		logging.Int("poolSize", p.cfg.ChannelPoolSize),
		logging.Duration("maxChannelAge", p.cfg.MaxChannelAge),
		logging.Int("warmChannels", warmed),
	)
}

func (p *publisher) createPooledChannel() *pooledChannel {
	ch := p.createConfirmChannel()
	if ch == nil {
		return nil
	}
	return &pooledChannel{ch: ch, createdAt: time.Now()}
}

// createConfirmChannel opens a channel and enables publisher confirms, each
// step bounded by its configured timeout.
func (p *publisher) createConfirmChannel() *amqp.Channel {
	p.connMu.RLock()
	conn := p.connection
	p.connMu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return nil
	}

	type channelResult struct {
		ch  *amqp.Channel
		err error
	}
	resultChan := make(chan channelResult)
	done := make(chan struct{})
	go func() {
		ch, err := conn.Channel()
		select {
		case resultChan <- channelResult{ch: ch, err: err}:
		case <-done:
			if ch != nil {
				_ = ch.Close()
			}
		}
	}()

	var ch *amqp.Channel
	select {
	case res := <-resultChan:
		close(done)
		if res.err != nil {
			p.log.Error(
				context.Background(),
				"failed to create RabbitMQ channel",
				logging.Err(res.err),
			)
			return nil
		}
		ch = res.ch
	case <-time.After(p.cfg.ChannelCreateTimeout):
		close(done)
		p.log.Error(
			context.Background(),
			"channel creation timed out",
			logging.Duration("timeout", p.cfg.ChannelCreateTimeout),
		)
		return nil
	}

	confirmChan := make(chan error, 1)
	go func() { confirmChan <- ch.Confirm(false) }()

	select {
	case err := <-confirmChan:
		if err != nil {
			p.log.Error(
				context.Background(),
				"failed to enable publisher confirms",
				logging.Err(err),
			)
			_ = ch.Close()
			return nil
		}
	case <-time.After(p.cfg.ConfirmEnableTimeout):
		p.log.Error(
			context.Background(),
			"enabling publisher confirms timed out",
			logging.Duration("timeout", p.cfg.ConfirmEnableTimeout),
		)
		_ = ch.Close()
		return nil
	}

	return ch
}

// borrowChannel borrows a pooled channel, replacing stale or closed ones. The
// internal publish path keeps the *pooledChannel wrapper so returning it needs
// no channelMeta lookups or allocations.
func (p *publisher) borrowChannel() (*pooledChannel, error) {
	if p.closed.Load() {
		return nil, errors.New("rabbitmq: publisher is closed")
	}

	p.poolMu.RLock()
	pool := p.channelPool
	if pool == nil {
		p.poolMu.RUnlock()
		return p.newChannelOrError()
	}

	select {
	case pch := <-pool:
		p.poolMu.RUnlock()
		if pch.ch.IsClosed() || time.Since(pch.createdAt) > p.cfg.MaxChannelAge {
			_ = pch.ch.Close()
			p.channelMeta.Delete(pch.ch)
			return p.newChannelOrError()
		}
		return pch, nil
	default:
		p.poolMu.RUnlock()
		return p.newChannelOrError()
	}
}

func (p *publisher) newChannelOrError() (*pooledChannel, error) {
	pch := p.createPooledChannel()
	if pch == nil {
		return nil, errors.New("rabbitmq: failed to create channel")
	}
	return pch, nil
}

// GetChannel borrows a channel from the pool, replacing stale or closed ones.
func (p *publisher) GetChannel() (*amqp.Channel, error) {
	pch, err := p.borrowChannel()
	if err != nil {
		return nil, err
	}
	// Record the creation time so ReturnChannel can restore the age of a
	// channel handed out through this bare-channel API.
	p.channelMeta.Store(pch.ch, pch.createdAt)
	return pch.ch, nil
}

// ReturnChannel returns a channel to the pool, closing it if the pool is full
// or the publisher is closed.
func (p *publisher) ReturnChannel(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		p.channelMeta.Delete(ch)
		return
	}
	createdAt := time.Now()
	if v, ok := p.channelMeta.Load(ch); ok {
		if t, ok := v.(time.Time); ok {
			createdAt = t
		}
	}
	p.returnPooled(&pooledChannel{ch: ch, createdAt: createdAt})
}

// returnPooled returns a borrowed channel to the pool, closing it if the pool
// is full, replaced, or the publisher is closed.
func (p *publisher) returnPooled(pch *pooledChannel) {
	ch := pch.ch
	if ch.IsClosed() || p.closed.Load() {
		_ = ch.Close()
		p.channelMeta.Delete(ch)
		return
	}
	p.poolMu.RLock()
	pool := p.channelPool
	if pool == nil {
		p.poolMu.RUnlock()
		_ = ch.Close()
		p.channelMeta.Delete(ch)
		return
	}
	select {
	case pool <- pch:
		p.poolMu.RUnlock()
	default:
		p.poolMu.RUnlock()
		_ = ch.Close()
		p.channelMeta.Delete(ch)
	}
}

// drainChannelPool closes and discards every pooled channel.
func (p *publisher) drainChannelPool() {
	p.poolMu.RLock()
	pool := p.channelPool
	p.poolMu.RUnlock()
	p.drainPool(pool)
}

func (p *publisher) drainPool(pool chan *pooledChannel) {
	if pool == nil {
		return
	}
	for {
		select {
		case pch := <-pool:
			_ = pch.ch.Close()
			p.channelMeta.Delete(pch.ch)
		default:
			return
		}
	}
}
