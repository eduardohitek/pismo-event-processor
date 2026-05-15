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

type ValidationError struct {
	Reason domain.QuarantineReason
	Detail string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

type Validator interface {
	Validate(raw []byte) (*domain.Event, error)
}

type SchemaValidator struct {
	schemas map[string]*jsonschema.Schema
}

// New compiles all *.json files in schemasDir at startup; fails fast if none are found.
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
		sch, err := compiler.Compile("file://" + filepath.ToSlash(absPath))
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

func (sv *SchemaValidator) Validate(raw []byte) (*domain.Event, error) {
	var ce cloudevents.Event
	err := json.Unmarshal(raw, &ce)
	if err != nil {
		return nil, &ValidationError{Reason: domain.ReasonInvalidEnvelope, Detail: err.Error()}
	}

	err = ce.Validate()
	if err != nil {
		return nil, &ValidationError{Reason: domain.ReasonInvalidEnvelope, Detail: err.Error()}
	}

	if ce.Subject() == "" {
		return nil, &ValidationError{Reason: domain.ReasonMissingTenant, Detail: "subject (tenant_id) is empty"}
	}

	sch, ok := sv.schemas[ce.Type()]
	if !ok {
		return nil, &ValidationError{Reason: domain.ReasonUnknownEventType, Detail: ce.Type()}
	}

	data := ce.Data()
	var payload any
	err = json.Unmarshal(data, &payload)
	if err != nil {
		return nil, &ValidationError{Reason: domain.ReasonInvalidPayload, Detail: err.Error()}
	}

	err = sch.Validate(payload)
	if err != nil {
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
		Data:            data,
		ReceivedAt:      time.Now().UTC(),
	}, nil
}
