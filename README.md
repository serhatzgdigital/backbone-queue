# backbone-queue

A small RabbitMQ (amqp091) wrapper for durable, at-least-once job
queues across backbone services. Sibling to `backbone-events`, which
is fire-and-forget Redis pub/sub for realtime fan-out — use
`backbone-queue` when a message must survive the consumer being down
(e.g. "create this notification row"), and `backbone-events` when a
dropped message is acceptable (e.g. "a socket should re-render").

## API

```go
c, err := queue.Dial(os.Getenv("RABBITMQ_URL")) // amqp://user:pass@host:5672/backbone
defer c.Close()

if err := c.DeclareQueue("notification.dispatch"); err != nil { ... }

ev := events.NewEvent("notification.dispatch.requested")
ev.Payload = events.MarshalPayload(job)
body, _ := json.Marshal(ev)
err = c.PublishJSON(ctx, "notification.dispatch", ev.EventID, body)

err = c.Consume(ctx, "notification.dispatch", 5, func(ctx context.Context, messageID string, body []byte) error {
	var ev events.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		return err // retried, then dead-lettered after 5 attempts
	}
	return handle(ctx, ev)
})
```

## Conventions

- **Vhost**: all backbone queues live under `/backbone`.
- **Queue naming**: `<service>.<purpose>`, e.g. `notification.dispatch`.
- **Dead-letter queue**: `<queue>.dlq`, declared automatically by
  `DeclareQueue`.
- **Envelope**: the message body is a `backbone-events.Event` JSON
  envelope (same shape used on the Redis pub/sub side), with the
  domain payload under `Payload`.
- **Idempotency**: `MessageId` is set to `Event.EventID`. Consumers
  that write to a DB should treat a primary-key/unique-constraint
  conflict on that ID as success, since redelivery can happen after a
  handler completes but before the ack lands.
