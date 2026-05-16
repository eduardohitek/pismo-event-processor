package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eduardohitek/pismo-event-processor/internal/domain"
	"github.com/oklog/ulid/v2"
)

// ErrDuplicate is returned when an event with the same ID already exists.
var ErrDuplicate = errors.New("duplicate event")

// EventStore persists valid events.
type EventStore interface {
	Save(ctx context.Context, event *domain.Event) error
}

// QuarantineStore persists rejected events.
type QuarantineStore interface {
	Save(ctx context.Context, q *domain.Quarantined) error
}

// DynamoDBClient is the subset of the DynamoDB API used by this package.
type DynamoDBClient interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// EventDynamo implements EventStore against DynamoDB.
type EventDynamo struct {
	client    DynamoDBClient
	tableName string
}

func NewEventStore(client DynamoDBClient, tableName string) *EventDynamo {
	return &EventDynamo{client: client, tableName: tableName}
}

func (e *EventDynamo) Save(ctx context.Context, event *domain.Event) error {
	item, err := attributevalue.MarshalMap(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	item["data"] = &types.AttributeValueMemberS{Value: string(event.Data)}

	_, err = e.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(e.tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(id)"),
	})
	if err != nil {
		var cce *types.ConditionalCheckFailedException
		if errors.As(err, &cce) {
			return ErrDuplicate
		}
		return fmt.Errorf("put event: %w", err)
	}
	return nil
}

// QuarantineDynamo implements QuarantineStore against DynamoDB.
type QuarantineDynamo struct {
	client    DynamoDBClient
	tableName string
}

func NewQuarantineStore(client DynamoDBClient, tableName string) *QuarantineDynamo {
	return &QuarantineDynamo{client: client, tableName: tableName}
}

func (q *QuarantineDynamo) Save(ctx context.Context, quarantined *domain.Quarantined) error {
	if quarantined.EventID == "" {
		quarantined.EventID = ulid.Make().String()
	}

	item, err := attributevalue.MarshalMap(quarantined)
	if err != nil {
		return fmt.Errorf("marshal quarantined: %w", err)
	}

	_, err = q.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(q.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("put quarantined: %w", err)
	}
	return nil
}
