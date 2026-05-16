package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eduardohitek/pismo-event-processor/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDynamo struct {
	putItemFn func(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

func (m *mockDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m.putItemFn(ctx, in, opts...)
}

func TestEventDynamo_Save_Success(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDynamo{
		putItemFn: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = in
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	store := NewEventStore(mock, "events")
	event := &domain.Event{ID: "01HXYZ", Data: []byte(`{"amount":100}`)}

	err := store.Save(context.Background(), event)
	require.NoError(t, err)
	assert.Equal(t, "attribute_not_exists(id)", *captured.ConditionExpression)
}

func TestEventDynamo_Save_Duplicate(t *testing.T) {
	mock := &mockDynamo{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, &types.ConditionalCheckFailedException{}
		},
	}

	store := NewEventStore(mock, "events")
	event := &domain.Event{ID: "01HXYZ", Data: []byte(`{}`)}

	err := store.Save(context.Background(), event)
	assert.ErrorIs(t, err, ErrDuplicate)
}

func TestEventDynamo_Save_TransientError(t *testing.T) {
	transient := errors.New("network timeout")
	mock := &mockDynamo{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			return nil, transient
		},
	}

	store := NewEventStore(mock, "events")
	event := &domain.Event{ID: "01HXYZ", Data: []byte(`{}`)}

	err := store.Save(context.Background(), event)
	require.Error(t, err)
	assert.ErrorContains(t, err, "network timeout")
}

func TestQuarantineDynamo_Save_GeneratesULID(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDynamo{
		putItemFn: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = in
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	store := NewQuarantineStore(mock, "quarantine")
	q := &domain.Quarantined{EventID: ""}

	err := store.Save(context.Background(), q)
	require.NoError(t, err)

	attr, ok := captured.Item["event_id"]
	require.True(t, ok, "event_id must be present in PutItem input")
	sv, ok := attr.(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.NotEmpty(t, sv.Value)
}

func TestQuarantineDynamo_Save_UsesProvidedID(t *testing.T) {
	var captured *dynamodb.PutItemInput
	mock := &mockDynamo{
		putItemFn: func(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
			captured = in
			return &dynamodb.PutItemOutput{}, nil
		},
	}

	store := NewQuarantineStore(mock, "quarantine")
	q := &domain.Quarantined{EventID: "provided-id"}

	err := store.Save(context.Background(), q)
	require.NoError(t, err)

	attr, ok := captured.Item["event_id"]
	require.True(t, ok)
	sv, ok := attr.(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "provided-id", sv.Value)
}
