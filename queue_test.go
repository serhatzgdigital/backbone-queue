package queue

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// These tests exercise only the pure retry-count header logic. They
// deliberately do NOT open a RabbitMQ connection — Dial/DeclareQueue/
// PublishJSON/Consume are integration-tested against a real broker
// elsewhere (see the Notification Service dispatch worker), not here.

func TestRetryCountFrom_NoHeaders(t *testing.T) {
	if got := retryCountFrom(nil); got != 0 {
		t.Fatalf("retryCountFrom(nil) = %d, want 0", got)
	}
}

func TestRetryCountFrom_MissingKey(t *testing.T) {
	headers := amqp.Table{"some-other-key": "value"}
	if got := retryCountFrom(headers); got != 0 {
		t.Fatalf("retryCountFrom(missing key) = %d, want 0", got)
	}
}

func TestRetryCountFrom_KnownIntTypes(t *testing.T) {
	cases := []struct {
		name string
		val  interface{}
		want int
	}{
		{"int", int(3), 3},
		{"int8", int8(3), 3},
		{"int16", int16(3), 3},
		{"int32", int32(3), 3},
		{"int64", int64(3), 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headers := amqp.Table{retryCountHeader: tc.val}
			if got := retryCountFrom(headers); got != tc.want {
				t.Fatalf("retryCountFrom(%v) = %d, want %d", tc.val, got, tc.want)
			}
		})
	}
}

func TestRetryCountFrom_UnexpectedType(t *testing.T) {
	// A string ended up in the header somehow (e.g. hand-crafted by an
	// external publisher) — must fail safe to 0, not panic or error.
	headers := amqp.Table{retryCountHeader: "not-a-number"}
	if got := retryCountFrom(headers); got != 0 {
		t.Fatalf("retryCountFrom(string) = %d, want 0 (fail-safe)", got)
	}
}

func TestWithRetryCount_SetsHeader(t *testing.T) {
	out := withRetryCount(nil, 1)
	if got := retryCountFrom(out); got != 1 {
		t.Fatalf("withRetryCount(nil, 1) round-trip = %d, want 1", got)
	}
}

func TestWithRetryCount_IncrementsAndPreservesOtherKeys(t *testing.T) {
	in := amqp.Table{
		retryCountHeader: int32(2),
		"trace-id":       "abc-123",
	}
	out := withRetryCount(in, 3)

	if got := retryCountFrom(out); got != 3 {
		t.Fatalf("retryCountFrom(incremented) = %d, want 3", got)
	}
	if out["trace-id"] != "abc-123" {
		t.Fatalf("withRetryCount dropped unrelated header trace-id: %v", out["trace-id"])
	}
}

func TestWithRetryCount_DoesNotMutateInput(t *testing.T) {
	in := amqp.Table{retryCountHeader: int32(1)}
	_ = withRetryCount(in, 5)
	if got := retryCountFrom(in); got != 1 {
		t.Fatalf("withRetryCount mutated its input: retryCountFrom(in) = %d, want 1 (unchanged)", got)
	}
}

// callHandler's panic recovery is pure logic too (no broker needed):
// a panicking handler must surface as a plain error, never crash the
// test/consume goroutine.

func TestCallHandler_RecoversPanic(t *testing.T) {
	err := callHandler(context.Background(), "msg-1", nil, func(ctx context.Context, messageID string, body []byte) error {
		panic("boom")
	})
	if err == nil {
		t.Fatal("callHandler returned nil error for a panicking handler, want a recovered error")
	}
}

func TestCallHandler_PropagatesError(t *testing.T) {
	wantErr := errors.New("handler failed")
	err := callHandler(context.Background(), "msg-1", nil, func(ctx context.Context, messageID string, body []byte) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("callHandler error = %v, want %v", err, wantErr)
	}
}

func TestCallHandler_PassesThroughMessageIDAndBody(t *testing.T) {
	wantBody := []byte(`{"x":1}`)
	var gotID string
	var gotBody []byte
	err := callHandler(context.Background(), "msg-42", wantBody, func(ctx context.Context, messageID string, body []byte) error {
		gotID = messageID
		gotBody = body
		return nil
	})
	if err != nil {
		t.Fatalf("callHandler unexpected error: %v", err)
	}
	if gotID != "msg-42" {
		t.Fatalf("messageID = %q, want %q", gotID, "msg-42")
	}
	if string(gotBody) != string(wantBody) {
		t.Fatalf("body = %q, want %q", gotBody, wantBody)
	}
}
