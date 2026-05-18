package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/oklog/ulid/v2"
)

const (
	paymentAuthorizedV1   = "com.pismo.payment.authorized.v1"
	monitoringHeartbeatV1 = "com.pismo.monitoring.heartbeat.v1"
)

var tenants = []string{"tenant-A", "tenant-B", "tenant-C"}

func main() {
	count := flag.Int("count", 5, "Number of valid events")
	invalid := flag.Int("invalid", 2, "Number of invalid events")
	scenario := flag.String("scenario", "", "Named scenario: idempotency | mixed-load | duplicate-burst")
	rate := flag.Int("rate", 0, "Messages per second (continuous mode)")
	duration := flag.String("duration", "30s", "Duration for rate mode (e.g. 30s)")
	errorRate := flag.Float64("error-rate", 0.1, "Fraction of invalid messages in rate mode")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: producer [flags]\n\nFlags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Scenarios (--scenario):
  idempotency     Sends the same event 3x to verify deduplication (expect 1 record)
  mixed-load      50 valid + 10 invalid events, randomly interleaved
  duplicate-burst 100 messages across 10 unique IDs (stress-tests idempotency)
`)
	}

	flag.Parse()

	var countSet, invalidSet bool
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "count" {
			countSet = true
		}
		if f.Name == "invalid" {
			invalidSet = true
		}
	})
	if *scenario != "" && (countSet || invalidSet) {
		fmt.Fprintln(os.Stderr, "error: --scenario is mutually exclusive with --count/--invalid")
		flag.Usage()
		os.Exit(1)
	}

	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		log.Fatal("SQS_QUEUE_URL environment variable is required")
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal("failed to load AWS config:", err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg)

	switch {
	case *rate > 0:
		runRateMode(ctx, sqsClient, queueURL, *rate, *duration, *errorRate)
	case *scenario != "":
		runScenario(ctx, sqsClient, queueURL, *scenario)
	default:
		runDefault(ctx, sqsClient, queueURL, *count, *invalid)
	}
}

func runDefault(ctx context.Context, client *sqs.Client, queueURL string, count, invalidCount int) {
	for i := range count {
		tenant := tenants[i%len(tenants)]
		var body []byte
		if i%2 == 0 {
			body = buildValidEventBytes("", tenant)
		} else {
			body = buildMonitoringEventBytes("", tenant)
		}
		if err := publish(ctx, client, queueURL, body); err != nil {
			log.Printf("publish valid[%d] failed: %v", i, err)
			continue
		}
		fmt.Printf("published valid event %d/%d\n", i+1, count)
	}
	for i := range invalidCount {
		err := publish(ctx, client, queueURL, buildInvalidEventBytes(i))
		if err != nil {
			log.Printf("publish invalid[%d] failed: %v", i, err)
			continue
		}
		fmt.Printf("published invalid event %d/%d\n", i+1, invalidCount)
	}
	fmt.Printf("done: %d valid, %d invalid\n", count, invalidCount)
}

func runScenario(ctx context.Context, client *sqs.Client, queueURL, name string) {
	switch name {
	case "idempotency":
		fixedID := ulid.Make().String()
		for i := range 3 {
			err := publish(ctx, client, queueURL, buildValidEventBytes(fixedID, tenants[0]))
			if err != nil {
				log.Printf("publish[%d] failed: %v", i, err)
			}
		}
		fmt.Printf("published 3× event ID=%s — expect 1 record in events table\n", fixedID)

	case "mixed-load":
		msgs := make([][]byte, 0, 60)
		for i := range 50 {
			msgs = append(msgs, buildValidEventBytes("", tenants[i%len(tenants)]))
		}
		for i := range 10 {
			msgs = append(msgs, buildInvalidEventBytes(i))
		}
		rand.Shuffle(len(msgs), func(i, j int) { msgs[i], msgs[j] = msgs[j], msgs[i] })
		for i, body := range msgs {
			if err := publish(ctx, client, queueURL, body); err != nil {
				log.Printf("publish[%d] failed: %v", i, err)
			}
		}
		fmt.Println("published 50 valid + 10 invalid events (mixed)")

	case "duplicate-burst":
		ids := make([]string, 10)
		for i := range ids {
			ids[i] = ulid.Make().String()
		}
		order := make([]int, 100)
		for i := range order {
			order[i] = i % 10
		}
		rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
		for i, idx := range order {
			// Each send uses a fresh transaction_id — verifies processor deduplicates
			// on CloudEvent ID (envelope), not on payload content.
			if err := publish(ctx, client, queueURL, buildValidEventBytes(ids[idx], tenants[0])); err != nil {
				log.Printf("publish[%d] failed: %v", i, err)
			}
		}
		fmt.Println("published 100 messages across 10 unique IDs — expect 10 records")

	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", name)
		flag.Usage()
		os.Exit(1)
	}
}

func runRateMode(ctx context.Context, client *sqs.Client, queueURL string, rate int, durationStr string, errRate float64) {
	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		log.Fatalf("invalid --duration %q: %v", durationStr, err)
	}
	dctx, cancel := context.WithDeadline(ctx, time.Now().Add(dur))
	defer cancel()

	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	var sent int
	for {
		select {
		case <-dctx.Done():
			fmt.Printf("\ndone: sent %d messages at %d msg/s for %s\n", sent, rate, durationStr)
			return
		case <-ticker.C:
			var body []byte
			if rand.Float64() < errRate {
				body = buildInvalidEventBytes(sent)
			} else {
				body = buildValidEventBytes("", tenants[sent%len(tenants)])
			}
			if err := publish(ctx, client, queueURL, body); err != nil {
				log.Printf("publish failed: %v", err)
			}
			sent++
			fmt.Printf("\rsent: %d", sent)
		}
	}
}

// baseEvent creates a CloudEvent with the common fields shared by all event types.
func baseEvent(eventType, tenantID string) cloudevents.Event {
	e := cloudevents.NewEvent()
	e.SetType(eventType)
	e.SetSource("producer")
	e.SetSubject(tenantID)
	e.SetDataContentType("application/json")
	e.SetTime(time.Now())
	return e
}

func buildValidEvent(id, tenantID string) cloudevents.Event {
	e := baseEvent(paymentAuthorizedV1, tenantID)
	e.SetID(id)
	_ = e.SetData("application/json", map[string]any{
		"transaction_id": ulid.Make().String(),
		"amount":         randomAmount(),
		"currency":       "BRL",
	})
	return e
}

func buildEventBytes(id, tenantID string, buildFn func(string, string) cloudevents.Event) []byte {
	if id == "" {
		id = ulid.Make().String()
	}
	b, _ := json.Marshal(buildFn(id, tenantID))
	return b
}

func buildValidEventBytes(id, tenantID string) []byte {
	return buildEventBytes(id, tenantID, buildValidEvent)
}

func buildMonitoringEvent(id, tenantID string) cloudevents.Event {
	e := baseEvent(monitoringHeartbeatV1, tenantID)
	e.SetID(id)
	statuses := []string{"ok", "degraded", "down"}
	_ = e.SetData("application/json", map[string]any{
		"service_name": "processor",
		"status":       statuses[rand.Intn(len(statuses))],
	})
	return e
}

func buildMonitoringEventBytes(id, tenantID string) []byte {
	return buildEventBytes(id, tenantID, buildMonitoringEvent)
}

func buildInvalidEventBytes(idx int) []byte {
	switch idx % 5 {
	case 0: // invalid JSON
		return []byte("{not-valid-json}")

	case 1: // missing required id field
		e := baseEvent(paymentAuthorizedV1, tenants[0])
		_ = e.SetData("application/json", map[string]any{
			"transaction_id": ulid.Make().String(),
			"amount":         randomAmount(),
			"currency":       "BRL",
		})
		b, _ := json.Marshal(e)
		return b

	case 2: // unknown event type
		e := baseEvent("com.pismo.unknown.v99", tenants[0])
		e.SetID(ulid.Make().String())
		_ = e.SetData("application/json", map[string]any{"foo": "bar"})
		b, _ := json.Marshal(e)
		return b

	case 3: // invalid payload (amount violates exclusiveMinimum: 0)
		e := baseEvent(paymentAuthorizedV1, tenants[0])
		e.SetID(ulid.Make().String())
		_ = e.SetData("application/json", map[string]any{
			"transaction_id": ulid.Make().String(),
			"amount":         -1,
			"currency":       "BRL",
		})
		b, _ := json.Marshal(e)
		return b

	default: // case 4: unregistered tenant (triggers triage quarantine)
		e := baseEvent(paymentAuthorizedV1, "tenant-unknown")
		e.SetID(ulid.Make().String())
		_ = e.SetData("application/json", map[string]any{
			"transaction_id": ulid.Make().String(),
			"amount":         randomAmount(),
			"currency":       "BRL",
		})
		b, _ := json.Marshal(e)
		return b
	}
}

func publish(ctx context.Context, client *sqs.Client, queueURL string, body []byte) error {
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	return err
}

func randomAmount() float64 {
	// Range: [1.00, 9999.99]
	cents := rand.Intn(999900) + 100
	return float64(cents) / 100.0
}
