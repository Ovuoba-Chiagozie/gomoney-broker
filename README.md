# In-Memory Message Broker (Go)

A small publish-subscribe broker for the GoMoney take-home assessment.

## Run

Requires Go 1.27+.

```bash
go run .          # runs the example sequence, prints 20 lines
go test ./...     # runs the tests
go test -race ./...   # runs the tests with the race detector
```

## Design

- **Broker** stores messages in a slice, so publish order is preserved.
- **Publisher** holds a broker and appends messages to it with `Publish`. It can publish any number of messages.
- **Subscriber** has an `id` and its own `position` (index of the next message to read), so subscribers never affect each other.
- **Subscribe** is keyed by subscriber id. Subscribing again is a no-op.
- **Read** returns all unread messages in order and moves the position to the end, so a message is never consumed twice.
- A subscriber that subscribes late starts at position 0, so it receives the full history. This is why both subscribers print all 10 messages in the example.
- A single `sync.Mutex` on the broker protects the message slice, the subscriber map and subscriber positions, so publishing and reading from different goroutines is safe.

## Assumptions / limitations

- Messages are plain strings.
- Messages are never deleted, so memory grows as more are published.
- Subscriber ids are assumed to be unique per broker.
