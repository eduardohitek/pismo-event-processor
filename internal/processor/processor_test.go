package processor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/eduardohitek/pismo-event-processor/internal/domain"
	"github.com/eduardohitek/pismo-event-processor/internal/messaging"
	"github.com/eduardohitek/pismo-event-processor/internal/storage"
	"github.com/eduardohitek/pismo-event-processor/internal/triage"
	"github.com/eduardohitek/pismo-event-processor/internal/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fakes ---

type fakeConsumer struct {
	mu         sync.Mutex
	messages   []messaging.Message
	ackCalls   []string
	receiveErr error
	ackErr     error
	callCount  int
}

func (f *fakeConsumer) Receive(ctx context.Context) ([]messaging.Message, error) {
	f.mu.Lock()
	f.callCount++
	call := f.callCount
	msgs := f.messages
	receiveErr := f.receiveErr
	f.mu.Unlock()

	if call == 1 {
		return msgs, receiveErr
	}
	// subsequent calls block until ctx is cancelled (simulates long-poll)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeConsumer) Ack(_ context.Context, msg messaging.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ackCalls = append(f.ackCalls, msg.ReceiptHandle)
	return f.ackErr
}

type fakeValidator struct {
	result *domain.Event
	valErr *validation.ValidationError
}

func (f *fakeValidator) Validate(_ []byte) (*domain.Event, error) {
	if f.valErr != nil {
		return nil, f.valErr
	}
	return f.result, nil
}

type fakeTriager struct {
	routing   *domain.Routing
	triageErr error
}

func (f *fakeTriager) Route(_ *domain.Event) (*domain.Routing, error) {
	if f.triageErr != nil {
		return nil, f.triageErr
	}
	if f.routing != nil {
		return f.routing, nil
	}
	return &domain.Routing{TargetClient: "tenant-A", Category: "transactional", Priority: 1}, nil
}

type fakeEventStore struct {
	mu      sync.Mutex
	saveErr error
	saved   []*domain.Event
	saveFn  func(*domain.Event)
}

func (f *fakeEventStore) Save(_ context.Context, event *domain.Event) error {
	if f.saveFn != nil {
		f.saveFn(event)
	}
	if f.saveErr != nil {
		return f.saveErr
	}
	f.mu.Lock()
	f.saved = append(f.saved, event)
	f.mu.Unlock()
	return nil
}

type fakeQuarantineStore struct {
	mu          sync.Mutex
	saveErr     error
	quarantined []*domain.Quarantined
}

func (f *fakeQuarantineStore) Save(_ context.Context, q *domain.Quarantined) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.mu.Lock()
	f.quarantined = append(f.quarantined, q)
	f.mu.Unlock()
	return nil
}

// --- helpers ---

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newProc(consumer *fakeConsumer, validator *fakeValidator, triager *fakeTriager, eventStore *fakeEventStore, quarantineStore *fakeQuarantineStore) *Processor {
	return New(Config{
		Consumer:        consumer,
		Validator:       validator,
		Triager:         triager,
		EventStore:      eventStore,
		QuarantineStore: quarantineStore,
		Logger:          discardLogger(),
		Workers:         1,
	})
}

func testMsg(receiptHandle string) messaging.Message {
	return messaging.Message{ID: "m1", Body: `{"key":"val"}`, ReceiptHandle: receiptHandle}
}

// --- test cases ---

func TestHandleValidEvent(t *testing.T) {
	event := &domain.Event{ID: "e1", Type: "com.test.v1", TenantID: "tenant-A"}
	consumer := &fakeConsumer{}
	eventStore := &fakeEventStore{}
	quarantineStore := &fakeQuarantineStore{}
	routing := &domain.Routing{TargetClient: "tenant-A", Category: "transactional", Priority: 1}
	triager := &fakeTriager{routing: routing}

	proc := newProc(consumer, &fakeValidator{result: event}, triager, eventStore, quarantineStore)
	proc.handle(context.Background(), testMsg("rh-1"))

	require.Len(t, eventStore.saved, 1)
	assert.Equal(t, routing, eventStore.saved[0].Routing)
	require.Len(t, consumer.ackCalls, 1)
	assert.Equal(t, "rh-1", consumer.ackCalls[0])
	assert.Empty(t, quarantineStore.quarantined)
}

func TestHandleQuarantineReasons(t *testing.T) {
	cases := []struct {
		name   string
		reason domain.QuarantineReason
		detail string
	}{
		{"malformed envelope", domain.ReasonInvalidEnvelope, "bad json"},
		{"invalid payload", domain.ReasonInvalidPayload, "schema mismatch"},
		{"unknown event type", domain.ReasonUnknownEventType, "com.unknown.v1"},
		{"missing tenant", domain.ReasonMissingTenant, "subject empty"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := &fakeConsumer{}
			eventStore := &fakeEventStore{}
			quarantineStore := &fakeQuarantineStore{}
			ve := &validation.ValidationError{Reason: tc.reason, Detail: tc.detail}

			proc := newProc(consumer, &fakeValidator{valErr: ve}, &fakeTriager{}, eventStore, quarantineStore)
			proc.handle(context.Background(), testMsg("rh-1"))

			assert.Empty(t, eventStore.saved)
			require.Len(t, quarantineStore.quarantined, 1)
			assert.Equal(t, tc.reason, quarantineStore.quarantined[0].Reason)
			require.Len(t, consumer.ackCalls, 1)
		})
	}
}

func TestHandleTriageErrors(t *testing.T) {
	cases := []struct {
		name   string
		reason domain.QuarantineReason
		detail string
	}{
		{"unregistered tenant", domain.ReasonUnregisteredTenant, "tenant not in allowlist"},
		{"no routing rule", domain.ReasonNoRoutingRule, "no rule matches event type"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := &fakeConsumer{}
			eventStore := &fakeEventStore{}
			quarantineStore := &fakeQuarantineStore{}
			event := &domain.Event{ID: "e1", Type: "com.test.v1", TenantID: "tenant-A"}
			te := &triage.TriageError{Reason: tc.reason, Detail: tc.detail}

			proc := newProc(consumer, &fakeValidator{result: event}, &fakeTriager{triageErr: te}, eventStore, quarantineStore)
			proc.handle(context.Background(), testMsg("rh-1"))

			assert.Empty(t, eventStore.saved)
			require.Len(t, quarantineStore.quarantined, 1)
			assert.Equal(t, tc.reason, quarantineStore.quarantined[0].Reason)
			require.Len(t, consumer.ackCalls, 1)
		})
	}
}

func TestHandleDuplicate(t *testing.T) {
	event := &domain.Event{ID: "e1"}
	consumer := &fakeConsumer{}
	eventStore := &fakeEventStore{saveErr: storage.ErrDuplicate}
	quarantineStore := &fakeQuarantineStore{}

	proc := newProc(consumer, &fakeValidator{result: event}, &fakeTriager{}, eventStore, quarantineStore)
	proc.handle(context.Background(), testMsg("rh-1"))

	assert.Empty(t, quarantineStore.quarantined)
	require.Len(t, consumer.ackCalls, 1)
	assert.Equal(t, "rh-1", consumer.ackCalls[0])
}

func TestHandleTransientError(t *testing.T) {
	event := &domain.Event{ID: "e1"}
	consumer := &fakeConsumer{}
	eventStore := &fakeEventStore{saveErr: errors.New("connection refused")}
	quarantineStore := &fakeQuarantineStore{}

	proc := newProc(consumer, &fakeValidator{result: event}, &fakeTriager{}, eventStore, quarantineStore)
	proc.handle(context.Background(), testMsg("rh-1"))

	// Critical invariant: transient store errors must NOT trigger an Ack.
	assert.Empty(t, consumer.ackCalls)
	assert.Empty(t, quarantineStore.quarantined)
}

func TestGracefulShutdown(t *testing.T) {
	saveStarted := make(chan struct{})
	saveDone := make(chan struct{})

	msg := messaging.Message{ID: "m1", Body: `{}`, ReceiptHandle: "rh-1"}
	consumer := &fakeConsumer{messages: []messaging.Message{msg}}
	event := &domain.Event{ID: "e1", Type: "com.test.v1", TenantID: "t1"}
	eventStore := &fakeEventStore{
		saveFn: func(_ *domain.Event) {
			close(saveStarted)
			<-saveDone
		},
	}
	quarantineStore := &fakeQuarantineStore{}

	proc := New(Config{
		Consumer:        consumer,
		Validator:       &fakeValidator{result: event},
		Triager:         &fakeTriager{},
		EventStore:      eventStore,
		QuarantineStore: quarantineStore,
		Logger:          discardLogger(),
		Workers:         1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- proc.Run(ctx) }()

	// Wait until the message is in-flight (Save has started).
	select {
	case <-saveStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("message was never dispatched to worker")
	}

	// Cancel while message is being processed.
	cancel()
	close(saveDone) // unblock Save so worker can finish

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}

	assert.Len(t, eventStore.saved, 1)
	assert.Len(t, consumer.ackCalls, 1)
}
