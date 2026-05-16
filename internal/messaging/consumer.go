package messaging

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Message struct {
	ID            string
	Body          string
	ReceiptHandle string
	ReceiveCount  int
}

type Consumer interface {
	Receive(ctx context.Context) ([]Message, error)
	Ack(ctx context.Context, msg Message) error
}

// SQSClient is the subset of the SQS API used by this package.
type SQSClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type SQSConsumer struct {
	client   SQSClient
	queueURL *string
	recvInput *sqs.ReceiveMessageInput
}

func NewSQSConsumer(client SQSClient, queueURL string) *SQSConsumer {
	qURL := aws.String(queueURL)
	return &SQSConsumer{
		client:   client,
		queueURL: qURL,
		recvInput: &sqs.ReceiveMessageInput{
			QueueUrl:            qURL,
			WaitTimeSeconds:     20,
			MaxNumberOfMessages: 10,
			AttributeNames:      []sqstypes.QueueAttributeName{"ApproximateReceiveCount"},
		},
	}
}

func (s *SQSConsumer) Receive(ctx context.Context) ([]Message, error) {
	out, err := s.client.ReceiveMessage(ctx, s.recvInput)
	if err != nil {
		return nil, fmt.Errorf("sqs receive: %w", err)
	}

	msgs := make([]Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		count, _ := strconv.Atoi(m.Attributes["ApproximateReceiveCount"])
		msgs = append(msgs, Message{
			ID:            aws.ToString(m.MessageId),
			Body:          aws.ToString(m.Body),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
			ReceiveCount:  count,
		})
	}
	return msgs, nil
}

func (s *SQSConsumer) Ack(ctx context.Context, msg Message) error {
	_, err := s.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      s.queueURL,
		ReceiptHandle: aws.String(msg.ReceiptHandle),
	})
	if err != nil {
		return fmt.Errorf("sqs delete: %w", err)
	}
	return nil
}
