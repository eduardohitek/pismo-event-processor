//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

const (
	eventsTable     = "events"
	quarantineTable = "quarantined_events"
)

var (
	sqsClient    *sqs.Client
	dynamoClient *dynamodb.Client
	queueURL     string
)

func TestMain(m *testing.M) {
	setDefault := func(key, val string) {
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
	setDefault("SQS_QUEUE_URL", "http://localhost:4566/000000000000/events")
	setDefault("AWS_ENDPOINT_URL", "http://localhost:4566")
	setDefault("AWS_REGION", "us-east-1")
	setDefault("AWS_ACCESS_KEY_ID", "test")
	setDefault("AWS_SECRET_ACCESS_KEY", "test")

	queueURL = os.Getenv("SQS_QUEUE_URL")
	endpointURL := os.Getenv("AWS_ENDPOINT_URL")

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(os.Getenv("AWS_REGION")))
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	sqsClient = sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = aws.String(endpointURL)
		}
	})
	dynamoClient = dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = aws.String(endpointURL)
		}
	})

	os.Exit(m.Run())
}

// ── helpers ──────────────────────────────────────────────────────────────────

func eventuallyAssert(t *testing.T, predicate func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

func clearTables(t *testing.T) {
	t.Helper()
	tables := []struct {
		name string
		pk   string
	}{
		{eventsTable, "id"},
		{quarantineTable, "event_id"},
	}
	ctx := context.Background()
	for _, tbl := range tables {
		out, err := dynamoClient.Scan(ctx, &dynamodb.ScanInput{
			TableName:            aws.String(tbl.name),
			ProjectionExpression: aws.String(tbl.pk),
		})
		if err != nil {
			t.Fatalf("clearTables scan %s: %v", tbl.name, err)
		}
		for _, item := range out.Items {
			_, err := dynamoClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
				TableName: aws.String(tbl.name),
				Key:       map[string]types.AttributeValue{tbl.pk: item[tbl.pk]},
			})
			if err != nil {
				t.Fatalf("clearTables delete from %s: %v", tbl.name, err)
			}
		}
	}
}

func publishEvent(t *testing.T, body []byte) {
	t.Helper()
	_, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		t.Fatalf("publishEvent: %v", err)
	}
}

func countRecords(t *testing.T, table string) int {
	t.Helper()
	out, err := dynamoClient.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: aws.String(table),
		Select:    types.SelectCount,
	})
	if err != nil {
		t.Fatalf("countRecords %s: %v", table, err)
	}
	return int(out.Count)
}

func scanTable(t *testing.T, table string) []map[string]types.AttributeValue {
	t.Helper()
	out, err := dynamoClient.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: aws.String(table),
	})
	if err != nil {
		t.Fatalf("scanTable %s: %v", table, err)
	}
	return out.Items
}

func getQuarantinedItem(t *testing.T) map[string]string {
	t.Helper()
	items := scanTable(t, quarantineTable)
	if len(items) == 0 {
		t.Fatal("no quarantined items found")
	}
	result := make(map[string]string, len(items[0]))
	for k, v := range items[0] {
		if sv, ok := v.(*types.AttributeValueMemberS); ok {
			result[k] = sv.Value
		}
	}
	return result
}

func extractField(items []map[string]types.AttributeValue, field string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if v, ok := item[field]; ok {
			if sv, ok := v.(*types.AttributeValueMemberS); ok {
				out = append(out, sv.Value)
			}
		}
	}
	return out
}

func buildValidCloudEvent(id, tenantID string) cloudevents.Event {
	e := cloudevents.NewEvent()
	e.SetID(id)
	e.SetType("com.pismo.payment.authorized.v1")
	e.SetSource("integration-test")
	e.SetSubject(tenantID)
	e.SetDataContentType("application/json")
	_ = e.SetData("application/json", map[string]any{
		"transaction_id": ulid.Make().String(),
		"amount":         42.50,
		"currency":       "BRL",
	})
	return e
}

func buildInvalidEvent(idx int) []byte {
	switch idx % 4 {
	case 0:
		return []byte("{not-valid-json}")
	case 1:
		e := cloudevents.NewEvent()
		e.SetType("com.pismo.payment.authorized.v1")
		e.SetSource("integration-test")
		e.SetSubject("tenant-001")
		e.SetDataContentType("application/json")
		_ = e.SetData("application/json", map[string]any{
			"transaction_id": ulid.Make().String(),
			"amount":         10.0,
			"currency":       "BRL",
		})
		b, _ := json.Marshal(e)
		return b
	case 2:
		e := cloudevents.NewEvent()
		e.SetID(ulid.Make().String())
		e.SetType("com.pismo.unknown.type")
		e.SetSource("integration-test")
		e.SetSubject("tenant-001")
		e.SetDataContentType("application/json")
		_ = e.SetData("application/json", map[string]any{"foo": "bar"})
		b, _ := json.Marshal(e)
		return b
	default: // invalid payload: amount violates exclusiveMinimum: 0
		e := cloudevents.NewEvent()
		e.SetID(ulid.Make().String())
		e.SetType("com.pismo.payment.authorized.v1")
		e.SetSource("integration-test")
		e.SetSubject("tenant-001")
		e.SetDataContentType("application/json")
		_ = e.SetData("application/json", map[string]any{
			"transaction_id": ulid.Make().String(),
			"amount":         -1,
			"currency":       "BRL",
		})
		b, _ := json.Marshal(e)
		return b
	}
}

func marshal(e cloudevents.Event) []byte {
	b, err := json.Marshal(e)
	if err != nil {
		panic(fmt.Sprintf("marshal: %v", err))
	}
	return b
}

// ── test cases ───────────────────────────────────────────────────────────────

func TestHappyPath(t *testing.T) {
	clearTables(t)

	event := buildValidCloudEvent(ulid.Make().String(), "tenant-001")
	publishEvent(t, marshal(event))

	eventuallyAssert(t, func() bool {
		return countRecords(t, eventsTable) == 1
	}, 10*time.Second, "event should appear in events table")

	assert.Equal(t, 0, countRecords(t, quarantineTable))
}

func TestQuarantine(t *testing.T) {
	clearTables(t)

	publishEvent(t, []byte("{not-valid-json}"))

	eventuallyAssert(t, func() bool {
		return countRecords(t, quarantineTable) == 1
	}, 10*time.Second, "invalid event should appear in quarantined_events")

	item := getQuarantinedItem(t)
	assert.Equal(t, "invalid_envelope", item["reason"])
	assert.Equal(t, 0, countRecords(t, eventsTable))
}

func TestIdempotency(t *testing.T) {
	clearTables(t)

	fixedID := ulid.Make().String()
	body := marshal(buildValidCloudEvent(fixedID, "tenant-001"))

	publishEvent(t, body)
	publishEvent(t, body)
	publishEvent(t, body)

	// Allow time for all 3 messages to be consumed before asserting.
	time.Sleep(2 * time.Second)

	eventuallyAssert(t, func() bool {
		return countRecords(t, eventsTable) == 1
	}, 15*time.Second, "exactly 1 record despite 3 publishes")
}

func TestMultiTenancy(t *testing.T) {
	clearTables(t)

	tenants := []string{"tenant-001", "tenant-002", "tenant-003"}
	for _, tenant := range tenants {
		publishEvent(t, marshal(buildValidCloudEvent(ulid.Make().String(), tenant)))
	}

	eventuallyAssert(t, func() bool {
		return countRecords(t, eventsTable) == 3
	}, 15*time.Second, "all 3 tenants should have records")

	items := scanTable(t, eventsTable)
	tenantIDs := extractField(items, "tenant_id")
	assert.ElementsMatch(t, tenants, tenantIDs)
}

func TestMixedValidInvalid(t *testing.T) {
	clearTables(t)

	for i := range 5 {
		publishEvent(t, marshal(buildValidCloudEvent(ulid.Make().String(), "tenant-001")))
	}
	for i := range 3 {
		publishEvent(t, buildInvalidEvent(i))
	}

	eventuallyAssert(t, func() bool {
		return countRecords(t, eventsTable) == 5 && countRecords(t, quarantineTable) == 3
	}, 20*time.Second, "5 valid + 3 quarantined")
}
