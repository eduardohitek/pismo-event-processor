package validation

import (
	"encoding/json"
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

func makeValidator(t *testing.T) *SchemaValidator {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "com.pismo.payment.authorized.v1.json")
	require.NoError(t, os.WriteFile(path, []byte(paymentSchema), 0600))
	v, err := New(dir)
	require.NoError(t, err)
	return v
}

func buildValidCloudEvent(txID string, amount float64, currency string) []byte {
	e := cloudevents.NewEvent()
	e.SetID("test-id-001")
	e.SetSource("test-source")
	e.SetType("com.pismo.payment.authorized.v1")
	e.SetSubject("tenant-123")
	_ = e.SetData("application/json", map[string]interface{}{
		"transaction_id": txID,
		"amount":         amount,
		"currency":       currency,
	})
	b, _ := json.Marshal(e)
	return b
}

func buildCloudEventWithoutID() []byte {
	return []byte(`{"specversion":"1.0","type":"com.pismo.payment.authorized.v1","source":"test-source","subject":"tenant-123","datacontenttype":"application/json","data":{"transaction_id":"tx-001","amount":99.90,"currency":"BRL"}}`)
}

func buildCloudEventWithEmptySubject() []byte {
	e := cloudevents.NewEvent()
	e.SetID("test-id-003")
	e.SetSource("test-source")
	e.SetType("com.pismo.payment.authorized.v1")
	e.SetSubject("")
	_ = e.SetData("application/json", map[string]interface{}{
		"transaction_id": "tx-003",
		"amount":         10.0,
		"currency":       "BRL",
	})
	b, _ := json.Marshal(e)
	return b
}

func buildCloudEventWithType(eventType string) []byte {
	e := cloudevents.NewEvent()
	e.SetID("test-id-004")
	e.SetSource("test-source")
	e.SetType(eventType)
	e.SetSubject("tenant-123")
	_ = e.SetData("application/json", map[string]interface{}{
		"transaction_id": "tx-004",
		"amount":         10.0,
		"currency":       "BRL",
	})
	b, _ := json.Marshal(e)
	return b
}

func buildCloudEventWithExtraField() []byte {
	e := cloudevents.NewEvent()
	e.SetID("test-id-005")
	e.SetSource("test-source")
	e.SetType("com.pismo.payment.authorized.v1")
	e.SetSubject("tenant-123")
	_ = e.SetData("application/json", map[string]interface{}{
		"transaction_id": "tx-005",
		"amount":         10.0,
		"currency":       "USD",
		"extra_field":    "not-allowed",
	})
	b, _ := json.Marshal(e)
	return b
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
			input:       buildValidCloudEvent("tx-001", 99.90, "BRL"),
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
			input:      buildCloudEventWithEmptySubject(),
			wantReason: domain.ReasonMissingTenant,
		},
		{
			name:       "unknown event type",
			input:      buildCloudEventWithType("com.pismo.unknown.v99"),
			wantReason: domain.ReasonUnknownEventType,
		},
		{
			name:       "negative amount",
			input:      buildValidCloudEvent("tx-002", -1, "BRL"),
			wantReason: domain.ReasonInvalidPayload,
		},
		{
			name:       "extra field (additionalProperties)",
			input:      buildCloudEventWithExtraField(),
			wantReason: domain.ReasonInvalidPayload,
		},
	}

	v := makeValidator(t)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			event, valErr := v.Validate(tc.input)

			if tc.wantEventID != "" {
				require.Nil(t, valErr, "expected no validation error")
				require.NotNil(t, event)
				assert.Equal(t, tc.wantEventID, event.ID)
				assert.NotEmpty(t, event.TenantID)
				assert.False(t, event.ReceivedAt.IsZero())
			} else {
				require.Nil(t, event, "expected nil event on error")
				require.NotNil(t, valErr)
				assert.Equal(t, tc.wantReason, valErr.Reason)
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
