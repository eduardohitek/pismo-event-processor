package domain

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	TenantID        string          `json:"tenant_id"`
	Time            time.Time       `json:"time"`
	SpecVersion     string          `json:"specversion"`
	DataContentType string          `json:"datacontenttype"`
	DataSchema      string          `json:"dataschema"`
	Data            json.RawMessage `json:"data"`
	ReceivedAt      time.Time       `json:"received_at"`
}

type QuarantineReason string

const (
	ReasonInvalidEnvelope  QuarantineReason = "invalid_envelope"
	ReasonUnknownEventType QuarantineReason = "unknown_event_type"
	ReasonInvalidPayload   QuarantineReason = "invalid_payload"
	ReasonMissingTenant    QuarantineReason = "missing_tenant"
)

type Quarantined struct {
	EventID        string           `json:"event_id"`
	Reason         QuarantineReason `json:"reason"`
	Detail         string           `json:"detail"`
	RawMessage     []byte           `json:"raw_message"`
	QuarantinedAt  time.Time        `json:"quarantined_at"`
}
