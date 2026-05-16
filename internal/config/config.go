package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	SQSQueueURL             string
	DynamoDBEventsTable     string
	DynamoDBQuarantineTable string
	AWSRegion               string
	AWSEndpointURL          string
	SchemasDir              string
	RoutingConfig           string
	ProcessorWorkers        int
	ShutdownGracePeriod     time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		AWSRegion:               getEnvOrDefault("AWS_REGION", "us-east-1"),
		AWSEndpointURL:          os.Getenv("AWS_ENDPOINT_URL"),
		SQSQueueURL:             os.Getenv("SQS_QUEUE_URL"),
		DynamoDBEventsTable:     os.Getenv("DYNAMODB_EVENTS_TABLE"),
		DynamoDBQuarantineTable: os.Getenv("DYNAMODB_QUARANTINE_TABLE"),
		SchemasDir:              getEnvOrDefault("SCHEMAS_DIR", "/schemas/payloads"),
		RoutingConfig:           getEnvOrDefault("ROUTING_CONFIG", "/config/routing.yaml"),
	}

	var errs []error
	if cfg.SQSQueueURL == "" {
		errs = append(errs, errors.New("SQS_QUEUE_URL is required"))
	}
	if cfg.DynamoDBEventsTable == "" {
		errs = append(errs, errors.New("DYNAMODB_EVENTS_TABLE is required"))
	}
	if cfg.DynamoDBQuarantineTable == "" {
		errs = append(errs, errors.New("DYNAMODB_QUARANTINE_TABLE is required"))
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	rawWorkers := os.Getenv("PROCESSOR_WORKERS")
	if rawWorkers != "" {
		workers, err := strconv.Atoi(rawWorkers)
		if err != nil || workers < 1 {
			return nil, fmt.Errorf("PROCESSOR_WORKERS must be a positive integer, got %q", rawWorkers)
		}
		cfg.ProcessorWorkers = workers
	} else {
		cfg.ProcessorWorkers = 5
	}

	rawGrace := os.Getenv("SHUTDOWN_GRACE_PERIOD")
	if rawGrace != "" {
		gracePeriod, err := time.ParseDuration(rawGrace)
		if err != nil {
			return nil, fmt.Errorf("SHUTDOWN_GRACE_PERIOD must be a valid duration, got %q", rawGrace)
		}
		cfg.ShutdownGracePeriod = gracePeriod
	} else {
		cfg.ShutdownGracePeriod = 30 * time.Second
	}

	return cfg, nil
}

func getEnvOrDefault(key, def string) string {
	v := os.Getenv(key)
	if v != "" {
		return v
	}
	return def
}
