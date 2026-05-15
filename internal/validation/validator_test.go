package validation

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/eduardohitek/pismo-event-processor/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const paymentSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "com.pismo.payment.authorized.v1",
  "type": "object",
  "required": ["transaction_id", "amount", "currency"],
  "properties": {
    "transaction_id": {"type": "string", "minLength": 1},
    "amount": {"type": "number", "exclusiveMinimum": 0},
    "currency": {"type": "string", "minLength": 3, "maxLength": 3}
  },
  "additionalProperties": false
}`

const (
	defaultEventType = "com.pismo.payment.authorized.v1"
	defaultSource    = "test-source"
	defaultSubject   = "tenant-123"
)

func makeValidator(t *testing.T) *SchemaValidator {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "com.pismo.payment.authorized.v1.json")
	require.NoError(t, os.WriteFile(path, []byte(paymentSchema), 0600))
	v, err := New(dir)
	require.NoError(t, err)
	return v
}

func mustBuildEvent(id, evtType, subject string, data map[string]any) []byte {
	e := cloudevents.NewEvent()
	e.SetID(id)
	e.SetSource(defaultSource)
	e.SetType(evtType)
	e.SetSubject(subject)
	if err := e.SetData("application/json", data); err != nil {
		panic("mustBuildEvent SetData: " + err.Error())
	}
	b, err := e.MarshalJSON()
	if err != nil {
		panic("mustBuildEvent MarshalJSON: " + err.Error())
	}
	return b
}

func validPayload(txID string, amount float64, currency string) map[string]any {
	return map[string]any{
		"transaction_id": txID,
		"amount":         amount,
		"currency":       currency,
	}
}

func buildCloudEventWithoutID() []byte {
	return []byte(`{"specversion":"1.0","type":"com.pismo.payment.authorized.v1","source":"test-source","subject":"tenant-123","datacontenttype":"application/json","data":{"transaction_id":"tx-001","amount":99.90,"currency":"BRL"}}`)
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name        string
		input       []byte
		wantEventID string
		wantReason  domain.QuarantineReason
	}{
		{
			name:        "valid event",
			input:       mustBuildEvent("test-id-001", defaultEventType, defaultSubject, validPayload("tx-001", 99.90, "BRL")),
			wantEventID: "test-id-001",
		},
		{
			name:       "malformed json",
			input:      []byte("{not-valid"),
			wantReason: domain.ReasonInvalidEnvelope,
		},
		{
			name:       "missing id",
			input:      buildCloudEventWithoutID(),
			wantReason: domain.ReasonInvalidEnvelope,
		},
		{
			name:       "empty subject (tenant)",
			input:      mustBuildEvent("test-id-003", defaultEventType, "", validPayload("tx-003", 10.0, "BRL")),
			wantReason: domain.ReasonMissingTenant,
		},
		{
			name:       "unknown event type",
			input:      mustBuildEvent("test-id-004", "com.pismo.unknown.v99", defaultSubject, validPayload("tx-004", 10.0, "BRL")),
			wantReason: domain.ReasonUnknownEventType,
		},
		{
			name:       "negative amount",
			input:      mustBuildEvent("test-id-002", defaultEventType, defaultSubject, validPayload("tx-002", -1, "BRL")),
			wantReason: domain.ReasonInvalidPayload,
		},
		{
			name: "extra field (additionalProperties)",
			input: mustBuildEvent("test-id-005", defaultEventType, defaultSubject, map[string]any{
				"transaction_id": "tx-005",
				"amount":         10.0,
				"currency":       "USD",
				"extra_field":    "not-allowed",
			}),
			wantReason: domain.ReasonInvalidPayload,
		},
	}

	v := makeValidator(t)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, err := v.Validate(tc.input)

			if tc.wantEventID != "" {
				require.NoError(t, err)
				require.NotNil(t, event)
				assert.Equal(t, tc.wantEventID, event.ID)
				assert.NotEmpty(t, event.TenantID)
				assert.False(t, event.ReceivedAt.IsZero())
			} else {
				require.Nil(t, event)
				require.Error(t, err)
				var ve *ValidationError
				require.True(t, errors.As(err, &ve))
				assert.Equal(t, tc.wantReason, ve.Reason)
			}
		})
	}
}

func TestNew_emptyDir(t *testing.T) {
	dir := t.TempDir()
	_, err := New(dir)
	assert.ErrorContains(t, err, "no schemas found")
}

func TestNew_invalidSchema(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	require.NoError(t, os.WriteFile(path, []byte(`{not valid json`), 0600))
	_, err := New(dir)
	assert.ErrorContains(t, err, "compile schema")
}
