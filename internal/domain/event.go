package domain

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID              string          `json:"id"              dynamodbav:"id"`
	Source          string          `json:"source"          dynamodbav:"source"`
	Type            string          `json:"type"            dynamodbav:"type"`
	TenantID        string          `json:"tenant_id"       dynamodbav:"tenant_id"`
	Time            time.Time       `json:"time"            dynamodbav:"time"`
	SpecVersion     string          `json:"specversion"     dynamodbav:"specversion"`
	DataContentType string          `json:"datacontenttype" dynamodbav:"datacontenttype"`
	DataSchema      string          `json:"dataschema"      dynamodbav:"dataschema"`
	Data            json.RawMessage `json:"data"            dynamodbav:"data"`
	ReceivedAt      time.Time       `json:"received_at"     dynamodbav:"received_at"`
}

type QuarantineReason string

const (
	ReasonInvalidEnvelope  QuarantineReason = "invalid_envelope"
	ReasonUnknownEventType QuarantineReason = "unknown_event_type"
	ReasonInvalidPayload   QuarantineReason = "invalid_payload"
	ReasonMissingTenant    QuarantineReason = "missing_tenant"
)

type Quarantined struct {
	EventID       string           `json:"event_id"      dynamodbav:"event_id"`
	Reason        QuarantineReason `json:"reason"        dynamodbav:"reason"`
	Detail        string           `json:"detail"        dynamodbav:"detail"`
	RawMessage    []byte           `json:"raw_message"   dynamodbav:"raw_message"`
	QuarantinedAt time.Time        `json:"quarantined_at" dynamodbav:"quarantined_at"`
}
