// Package queue is a small RabbitMQ (amqp091) wrapper shared by
// backbone services that need a durable, at-least-once job queue —
// as opposed to backbone-events, which is fire-and-forget pub/sub
// over Redis for realtime fan-out. Use this package when a message
// MUST be processed even if the consumer is down when it's produced
// (e.g. "create this notification row"), and use backbone-events
// when a dropped message is acceptable (e.g. "a socket should
// re-render").
//
// Design notes:
//
//   - Queues are durable classic queues. Every queue declared via
//     DeclareQueue gets a companion dead-letter queue named
//     `<queue>.dlq` so operators have somewhere to look when a
//     handler keeps failing, instead of the message silently
//     vanishing after the retry budget is exhausted.
//   - Retries are counted in a message header (`x-retry-count`)
//     rather than relying on RabbitMQ's native x-death/DLX
//     bookkeeping. That keeps the retry contract simple and
//     independent of broker version quirks: Consume owns the whole
//     retry decision in Go, not in queue topology.
//   - Publish is confirm-mode: PublishJSON does not return until the
//     broker has acked the message, so a producer that gets a nil
//     error can trust the message is durably queued.
//   - The publish path and the consume path each own an independent
//     AMQP connection. That means a broker blip that interrupts an
//     in-flight Consume loop's reconnect dance never blocks or races
//     a concurrent PublishJSON call (and vice versa) — they only
//     share the target URL, not a channel or a mutex.
package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// retryCountHeader carries the redelivery attempt count. We manage
	// it ourselves (rather than parsing RabbitMQ's x-death array)
	// because it's a single int we fully control end to end.
	retryCountHeader = "x-retry-count"

	// dlqSuffix is appended to a queue name to get its dead-letter
	// companion. Kept as a package constant (not a knob) so producers
	// and consumers across services can never drift on the name.
	dlqSuffix = ".dlq"

	// consumePrefetch bounds how many unacked deliveries a single
	// Consume loop holds at once. 10 is a deliberately small default:
	// these are job queues (notification dispatch, etc.), not a high
	// throughput stream, and a small prefetch keeps one slow handler
	// from starving fair dispatch across consumers.
	consumePrefetch = 10

	// minBackoff / maxBackoff bound the exponential backoff used when
	// Consume redials after losing its connection or channel.
	minBackoff = 1 * time.Second
	maxBackoff = 30 * time.Second
)

// Client wraps a RabbitMQ connection and a dedicated confirm-mode
// publish channel. It is safe for concurrent use: PublishJSON and
// DeclareQueue serialize on an internal mutex; Consume dials and
// manages its own independent connection (see package doc).
type Client struct {
	url string

	mu    sync.Mutex // guards conn + pubCh below
	conn  *amqp.Connection
	pubCh *amqp.Channel
}

// Dial connects to RabbitMQ. url must include the vhost, e.g.
// amqp://user:pass@rabbitmq.railway.internal:5672/backbone
func Dial(url string) (*Client, error) {
	c := &Client{url: url}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(); err != nil {
		return nil, err
	}
	return c, nil
}

// connectLocked (re)establishes c.conn and c.pubCh, closing whatever
// was there before on a best-effort basis. Caller must hold c.mu.
func (c *Client) connectLocked() error {
	if c.pubCh != nil {
		_ = c.pubCh.Close()
		c.pubCh = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("queue: dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("queue: open publish channel: %w", err)
	}
	// Publisher confirms: PublishJSON blocks on the broker's ack, so a
	// nil error from it is a durability guarantee, not just "we wrote
	// it to a socket".
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("queue: enable confirm mode: %w", err)
	}

	c.conn = conn
	c.pubCh = ch
	return nil
}

// ensureConnLocked redials if the current connection/channel is gone
// or closed. Caller must hold c.mu.
func (c *Client) ensureConnLocked() error {
	if c.conn != nil && !c.conn.IsClosed() && c.pubCh != nil {
		return nil
	}
	return c.connectLocked()
}

// Close tears down the publish connection. Safe to call once; a
// Consume loop in flight on another goroutine is unaffected since it
// owns its own connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	if c.pubCh != nil {
		if e := c.pubCh.Close(); e != nil && !errors.Is(e, amqp.ErrClosed) {
			err = e
		}
		c.pubCh = nil
	}
	if c.conn != nil {
		if e := c.conn.Close(); e != nil && !errors.Is(e, amqp.ErrClosed) {
			err = e
		}
		c.conn = nil
	}
	return err
}

// DeclareQueue declares a durable classic queue `name` plus its
// dead-letter companion `name+".dlq"` (also durable). Idempotent —
// RabbitMQ's QueueDeclare is itself idempotent as long as the
// arguments match what's already there, which they always do here
// since we never pass per-call arguments.
func (c *Client) DeclareQueue(name string) error {
	if name == "" {
		return errors.New("queue: name is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ensureConnLocked(); err != nil {
		return err
	}
	if _, err := c.pubCh.QueueDeclare(name, true, false, false, false, nil); err != nil {
		return fmt.Errorf("queue: declare %s: %w", name, err)
	}
	dlq := name + dlqSuffix
	if _, err := c.pubCh.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return fmt.Errorf("queue: declare %s: %w", dlq, err)
	}
	return nil
}

// PublishJSON publishes body to queue with persistent delivery mode,
// content-type application/json, MessageId=messageID, and waits for
// publisher confirm.
func (c *Client) PublishJSON(ctx context.Context, queueName, messageID string, body []byte) error {
	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    messageID,
		Body:         body,
	}

	c.mu.Lock()
	if err := c.ensureConnLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	ch := c.pubCh
	c.mu.Unlock()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queueName, false, false, msg)
	if err != nil {
		// The channel/connection may have died between ensureConnLocked
		// and this call (e.g. broker restart mid-request). Retry once
		// against a freshly dialed connection so a single transient
		// blip doesn't fail the caller outright.
		c.mu.Lock()
		rerr := c.connectLocked()
		if rerr != nil {
			c.mu.Unlock()
			return fmt.Errorf("queue: publish: %w (reconnect failed: %v)", err, rerr)
		}
		ch = c.pubCh
		c.mu.Unlock()

		confirm, err = ch.PublishWithDeferredConfirmWithContext(ctx, "", queueName, false, false, msg)
		if err != nil {
			return fmt.Errorf("queue: publish: %w", err)
		}
	}

	ok, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("queue: publish confirm: %w", err)
	}
	if !ok {
		return errors.New("queue: publish nacked by broker")
	}
	return nil
}

// Consume blocks, delivering messages to handler with manual ack.
// On handler error: if the message's retry count (header "x-retry-count",
// int) < maxRetries, republish to the same queue with the header
// incremented; otherwise publish to queue+".dlq". In both cases the
// original delivery is acked afterwards (never requeue via nack).
// Returns when ctx is cancelled. On connection/channel loss it
// redials with exponential backoff (cap 30s) and resumes consuming.
func (c *Client) Consume(ctx context.Context, queueName string, maxRetries int, handler func(ctx context.Context, messageID string, body []byte) error) error {
	backoff := minBackoff
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := c.consumeOnce(ctx, queueName, maxRetries, handler)
		if err == nil {
			// consumeOnce only returns a nil error when ctx was
			// cancelled while we were idle between deliveries.
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("queue: consume %s: connection lost, redialing in %s: %v", queueName, backoff, err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// consumeOnce dials its own connection (independent of the publish
// connection — see package doc), consumes until either ctx is
// cancelled (nil return) or the connection/channel drops (non-nil
// return so Consume's backoff loop redials).
func (c *Client) consumeOnce(ctx context.Context, queueName string, maxRetries int, handler func(ctx context.Context, messageID string, body []byte) error) error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("queue: consume dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("queue: consume channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(consumePrefetch, 0, false); err != nil {
		return fmt.Errorf("queue: qos: %w", err)
	}

	deliveries, err := ch.ConsumeWithContext(ctx, queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue: consume: %w", err)
	}

	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			return nil
		case cerr, ok := <-connClosed:
			if !ok || cerr == nil {
				return errors.New("queue: connection closed")
			}
			return fmt.Errorf("queue: connection closed: %w", cerr)
		case cerr, ok := <-chClosed:
			if !ok || cerr == nil {
				return errors.New("queue: channel closed")
			}
			return fmt.Errorf("queue: channel closed: %w", cerr)
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("queue: delivery channel closed")
			}
			c.handleDelivery(ctx, ch, queueName, maxRetries, d, handler)
		}
	}
}

// handleDelivery runs handler with panic recovery and resolves the
// retry/dead-letter decision. The original delivery is always acked
// at the end — we never nack-with-requeue, since that would hand the
// same message straight back to us (or another consumer) with no
// backoff and no bound, turning a bad message into a hot loop.
func (c *Client) handleDelivery(ctx context.Context, ch *amqp.Channel, queueName string, maxRetries int, d amqp.Delivery, handler func(ctx context.Context, messageID string, body []byte) error) {
	if err := callHandler(ctx, d.MessageId, d.Body, handler); err != nil {
		retryCount := retryCountFrom(d.Headers)
		nextHeaders := withRetryCount(d.Headers, retryCount+1)

		target := queueName
		if retryCount >= maxRetries {
			target = queueName + dlqSuffix
			log.Printf("queue: %s: handler failed permanently after %d attempt(s), dead-lettering msg_id=%s err=%v",
				queueName, retryCount+1, d.MessageId, err)
		} else {
			log.Printf("queue: %s: handler failed (attempt %d/%d), retrying msg_id=%s err=%v",
				queueName, retryCount+1, maxRetries, d.MessageId, err)
		}

		if pubErr := ch.PublishWithContext(ctx, "", target, false, false, amqp.Publishing{
			ContentType:  d.ContentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    d.MessageId,
			Headers:      nextHeaders,
			Body:         d.Body,
		}); pubErr != nil {
			log.Printf("queue: %s: republish to %s failed, message dropped msg_id=%s err=%v",
				queueName, target, d.MessageId, pubErr)
		}
	}
	_ = d.Ack(false)
}

// callHandler wraps handler with panic recovery so one malformed
// message can never take down the whole Consume loop — a panic is
// simply treated as a handler error and flows through the same
// retry/dead-letter path.
func callHandler(ctx context.Context, messageID string, body []byte, handler func(ctx context.Context, messageID string, body []byte) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("queue: handler panic: %v", r)
		}
	}()
	return handler(ctx, messageID, body)
}

// retryCountFrom reads the x-retry-count header. Missing header,
// nil headers, or an unexpected value type are all treated as "first
// attempt" (0) — the safe failure mode, since undercounting only
// costs one extra retry while overcounting could skip straight past
// the retry budget into the DLQ.
func retryCountFrom(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	v, ok := headers[retryCountHeader]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	default:
		return 0
	}
}

// withRetryCount returns a copy of headers with x-retry-count set to
// n. It copies rather than mutates the delivery's own Headers map,
// since that map is owned by amqp091 internals and mutating it in
// place is not part of any documented contract.
func withRetryCount(headers amqp.Table, n int) amqp.Table {
	out := amqp.Table{}
	for k, v := range headers {
		out[k] = v
	}
	out[retryCountHeader] = int32(n)
	return out
}
