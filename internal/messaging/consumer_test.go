package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSQS struct {
	receiveFn func(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	deleteFn  func(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

func (m *mockSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return m.receiveFn(ctx, in, opts...)
}

func (m *mockSQS) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return m.deleteFn(ctx, in, opts...)
}

func TestSQSConsumer_Receive_TwoMessages(t *testing.T) {
	mock := &mockSQS{
		receiveFn: func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			return &sqs.ReceiveMessageOutput{
				Messages: []sqstypes.Message{
					{
						MessageId:     aws.String("msg-1"),
						Body:          aws.String(`{"event":"a"}`),
						ReceiptHandle: aws.String("rh-1"),
						Attributes:    map[string]string{"ApproximateReceiveCount": "1"},
					},
					{
						MessageId:     aws.String("msg-2"),
						Body:          aws.String(`{"event":"b"}`),
						ReceiptHandle: aws.String("rh-2"),
						Attributes:    map[string]string{"ApproximateReceiveCount": "3"},
					},
				},
			}, nil
		},
	}

	consumer := NewSQSConsumer(mock, "https://sqs.test/queue")
	msgs, err := consumer.Receive(context.Background())

	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, Message{ID: "msg-1", Body: `{"event":"a"}`, ReceiptHandle: "rh-1", ReceiveCount: 1}, msgs[0])
	assert.Equal(t, Message{ID: "msg-2", Body: `{"event":"b"}`, ReceiptHandle: "rh-2", ReceiveCount: 3}, msgs[1])
}

func TestSQSConsumer_Receive_Empty(t *testing.T) {
	mock := &mockSQS{
		receiveFn: func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{}}, nil
		},
	}

	consumer := NewSQSConsumer(mock, "https://sqs.test/queue")
	msgs, err := consumer.Receive(context.Background())

	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func TestSQSConsumer_Ack(t *testing.T) {
	var capturedHandle string
	mock := &mockSQS{
		deleteFn: func(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
			capturedHandle = aws.ToString(in.ReceiptHandle)
			return &sqs.DeleteMessageOutput{}, nil
		},
	}

	consumer := NewSQSConsumer(mock, "https://sqs.test/queue")
	err := consumer.Ack(context.Background(), Message{ReceiptHandle: "rh-abc"})

	require.NoError(t, err)
	assert.Equal(t, "rh-abc", capturedHandle)
}

func TestSQSConsumer_Receive_ContextCanceled(t *testing.T) {
	mock := &mockSQS{
		receiveFn: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			return nil, ctx.Err()
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	consumer := NewSQSConsumer(mock, "https://sqs.test/queue")
	_, err := consumer.Receive(ctx)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSQSConsumer_Receive_SQSError(t *testing.T) {
	sqsErr := errors.New("connection refused")
	mock := &mockSQS{
		receiveFn: func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			return nil, sqsErr
		},
	}

	consumer := NewSQSConsumer(mock, "https://sqs.test/queue")
	_, err := consumer.Receive(context.Background())

	require.Error(t, err)
	assert.ErrorContains(t, err, "connection refused")
}
