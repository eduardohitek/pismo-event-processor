package validation

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/eduardohitek/pismo-event-processor/internal/domain"
	"github.com/santhosh-tekuri/jsonschema/v5"
	_ "github.com/santhosh-tekuri/jsonschema/v5/httploader"
)

// ValidationError carries the quarantine reason and diagnostic detail.
type ValidationError struct {
	Reason domain.QuarantineReason
	Detail string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

// Validator validates a raw SQS message body and returns a parsed Event.
type Validator interface {
	Validate(raw []byte) (*domain.Event, *ValidationError)
}

// SchemaValidator implements Validator using CloudEvents envelope + JSON Schema payload validation.
type SchemaValidator struct {
	schemas map[string]*jsonschema.Schema
}

// New loads and compiles all *.json schemas from schemasDir.
// Returns error if schemasDir is unreadable or any schema fails to compile.
func New(schemasDir string) (*SchemaValidator, error) {
	entries, err := os.ReadDir(schemasDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read schemas dir: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	schemas := make(map[string]*jsonschema.Schema)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		absPath := filepath.Join(schemasDir, entry.Name())
		sch, err := compiler.Compile("file://" + absPath)
		if err != nil {
			return nil, fmt.Errorf("compile schema %s: %w", entry.Name(), err)
		}
		key := strings.TrimSuffix(entry.Name(), ".json")
		schemas[key] = sch
	}

	if len(schemas) == 0 {
		return nil, fmt.Errorf("no schemas found in %s", schemasDir)
	}

	return &SchemaValidator{schemas: schemas}, nil
}

// Validate parses and validates a raw CloudEvents JSON message.
func (sv *SchemaValidator) Validate(raw []byte) (*domain.Event, *ValidationError) {
	var ce cloudevents.Event
	if err := json.Unmarshal(raw, &ce); err != nil {
		return nil, &ValidationError{Reason: domain.ReasonInvalidEnvelope, Detail: err.Error()}
	}

	if err := ce.Validate(); err != nil {
		return nil, &ValidationError{Reason: domain.ReasonInvalidEnvelope, Detail: err.Error()}
	}

	if ce.Subject() == "" {
		return nil, &ValidationError{Reason: domain.ReasonMissingTenant, Detail: "subject (tenant_id) is empty"}
	}

	sch, ok := sv.schemas[ce.Type()]
	if !ok {
		return nil, &ValidationError{Reason: domain.ReasonUnknownEventType, Detail: ce.Type()}
	}

	var payload interface{}
	if err := json.Unmarshal(ce.Data(), &payload); err != nil {
		return nil, &ValidationError{Reason: domain.ReasonInvalidPayload, Detail: err.Error()}
	}

	if err := sch.Validate(payload); err != nil {
		return nil, &ValidationError{Reason: domain.ReasonInvalidPayload, Detail: err.Error()}
	}

	return &domain.Event{
		ID:              ce.ID(),
		Source:          ce.Source(),
		Type:            ce.Type(),
		TenantID:        ce.Subject(),
		Time:            ce.Time(),
		SpecVersion:     ce.SpecVersion(),
		DataContentType: ce.DataContentType(),
		DataSchema:      ce.DataSchema(),
		Data:            ce.Data(),
		ReceivedAt:      time.Now().UTC(),
	}, nil
}
