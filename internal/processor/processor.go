package processor

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/eduardohitek/pismo-event-processor/internal/domain"
	"github.com/eduardohitek/pismo-event-processor/internal/messaging"
	"github.com/eduardohitek/pismo-event-processor/internal/storage"
	"github.com/eduardohitek/pismo-event-processor/internal/triage"
	"github.com/eduardohitek/pismo-event-processor/internal/validation"
)

type Triager interface {
	Route(evt *domain.Event) (*domain.Routing, error)
}

type Config struct {
	Consumer        messaging.Consumer
	Validator       validation.Validator
	Triager         Triager
	EventStore      storage.EventStore
	QuarantineStore storage.QuarantineStore
	Logger          *slog.Logger
	Workers         int
}

type Processor struct{ cfg Config }

func New(cfg Config) *Processor {
	return &Processor{cfg: cfg}
}

func (p *Processor) Run(ctx context.Context) error {
	jobs := make(chan messaging.Message, p.cfg.Workers)

	var wg sync.WaitGroup
	for range p.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				p.handle(ctx, msg)
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil
		default:
			subCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
			msgs, err := p.cfg.Consumer.Receive(subCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				p.cfg.Logger.Error("receive error", "error", err)
				continue
			}
			for _, msg := range msgs {
				select {
				case jobs <- msg:
				case <-ctx.Done():
					close(jobs)
					wg.Wait()
					return nil
				}
			}
		}
	}
}

func (p *Processor) ack(ctx context.Context, msg messaging.Message, attrs ...any) {
	err := p.cfg.Consumer.Ack(ctx, msg)
	if err != nil {
		args := append([]any{"message_id", msg.ID, "error", err}, attrs...)
		p.cfg.Logger.Error("ack failed", args...)
	}
}

func (p *Processor) quarantine(ctx context.Context, msg messaging.Message, reason domain.QuarantineReason, detail, stage string) {
	q := &domain.Quarantined{
		Reason:     reason,
		Detail:     detail,
		RawMessage: msg.Body,
	}
	if err := p.cfg.QuarantineStore.Save(ctx, q); err != nil {
		p.cfg.Logger.Error("quarantine save failed", "message_id", msg.ID, "error", err)
	}
	p.ack(ctx, msg)
	p.cfg.Logger.Warn("quarantined", "stage", stage, "message_id", msg.ID, "reason", reason, "detail", detail)
}

func (p *Processor) handle(ctx context.Context, msg messaging.Message) {
	event, err := p.cfg.Validator.Validate([]byte(msg.Body))
	if err != nil {
		var ve *validation.ValidationError
		if !errors.As(err, &ve) {
			p.cfg.Logger.Error("unexpected validation error", "message_id", msg.ID, "error", err)
			return
		}
		p.quarantine(ctx, msg, ve.Reason, ve.Detail, "validate")
		return
	}

	routing, err := p.cfg.Triager.Route(event)
	if err != nil {
		var te *triage.TriageError
		if !errors.As(err, &te) {
			p.cfg.Logger.Error("unexpected triage error", "message_id", msg.ID, "error", err)
			return
		}
		p.quarantine(ctx, msg, te.Reason, te.Detail, "triage")
		return
	}
	event.Routing = routing

	saveErr := p.cfg.EventStore.Save(ctx, event)
	if saveErr != nil {
		if errors.Is(saveErr, storage.ErrDuplicate) {
			p.cfg.Logger.Info("duplicate, skipping",
				"stage", "persist",
				"event_id", event.ID, "event_type", event.Type,
				"tenant_id", event.TenantID, "message_id", msg.ID)
			p.ack(ctx, msg)
			return
		}
		// Transient error — do NOT ack; SQS will redeliver after VisibilityTimeout.
		p.cfg.Logger.Error("event store error",
			"stage", "persist",
			"event_id", event.ID, "event_type", event.Type,
			"tenant_id", event.TenantID, "message_id", msg.ID, "error", saveErr)
		return
	}

	p.ack(ctx, msg, "event_id", event.ID, "event_type", event.Type, "tenant_id", event.TenantID)
	p.cfg.Logger.Info("event processed",
		"stage", "persist",
		"event_id", event.ID, "event_type", event.Type,
		"tenant_id", event.TenantID, "message_id", msg.ID)
}
