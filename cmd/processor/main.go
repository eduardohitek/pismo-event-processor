package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eduardohitek/pismo-event-processor/internal/config"
	"github.com/eduardohitek/pismo-event-processor/internal/messaging"
	"github.com/eduardohitek/pismo-event-processor/internal/processor"
	"github.com/eduardohitek/pismo-event-processor/internal/storage"
	"github.com/eduardohitek/pismo-event-processor/internal/validation"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config:", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		log.Fatal("failed to load AWS config:", err)
	}

	// Per-service endpoint override — used to point at LocalStack when AWS_ENDPOINT_URL is set.
	var sqsOpts []func(*sqs.Options)
	var dynamoOpts []func(*dynamodb.Options)
	if cfg.AWSEndpointURL != "" {
		sqsOpts = append(sqsOpts, func(o *sqs.Options) { o.BaseEndpoint = aws.String(cfg.AWSEndpointURL) })
		dynamoOpts = append(dynamoOpts, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(cfg.AWSEndpointURL) })
	}

	sqsClient := sqs.NewFromConfig(awsCfg, sqsOpts...)
	dynamoClient := dynamodb.NewFromConfig(awsCfg, dynamoOpts...)

	schemasDir := "/schemas/payloads"
	if d := os.Getenv("SCHEMAS_DIR"); d != "" {
		schemasDir = d
	}
	validator, err := validation.New(schemasDir)
	if err != nil {
		log.Fatal("failed to load schemas:", err)
	}

	consumer := messaging.NewSQSConsumer(sqsClient, cfg.SQSQueueURL)
	eventStore := storage.NewEventStore(dynamoClient, cfg.DynamoDBEventsTable)
	quarantineStore := storage.NewQuarantineStore(dynamoClient, cfg.DynamoDBQuarantineTable)

	proc := processor.New(processor.Config{
		Consumer:        consumer,
		Validator:       validator,
		EventStore:      eventStore,
		QuarantineStore: quarantineStore,
		Logger:          logger,
		Workers:         cfg.ProcessorWorkers,
	})

	logger.Info("processor starting", "workers", cfg.ProcessorWorkers)
	if err := proc.Run(ctx); err != nil {
		logger.Error("processor error", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer cancel()
	<-shutdownCtx.Done()
	logger.Info("processor stopped")
}
