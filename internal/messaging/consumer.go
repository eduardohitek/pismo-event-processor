package messaging

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Message is a provider-neutral representation of a queue message.
type Message struct {
	ID            string
	Body          string
	ReceiptHandle string
	ReceiveCount  int
}

// Consumer reads messages from a queue and acknowledges processed ones.
type Consumer interface {
	Receive(ctx context.Context) ([]Message, error)
	Ack(ctx context.Context, msg Message) error
}

// SQSClient is the subset of the SQS API used by this package.
type SQSClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// SQSConsumer implements Consumer against Amazon SQS.
type SQSConsumer struct {
	client   SQSClient
	queueURL string
}

func NewSQSConsumer(client SQSClient, queueURL string) *SQSConsumer {
	return &SQSConsumer{client: client, queueURL: queueURL}
}

func (s *SQSConsumer) Receive(ctx context.Context) ([]Message, error) {
	out, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.queueURL),
		WaitTimeSeconds:     20,
		MaxNumberOfMessages: 10,
		AttributeNames:      []sqstypes.QueueAttributeName{"ApproximateReceiveCount"},
	})
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
		QueueUrl:      aws.String(s.queueURL),
		ReceiptHandle: aws.String(msg.ReceiptHandle),
	})
	if err != nil {
		return fmt.Errorf("sqs delete: %w", err)
	}
	return nil
}
