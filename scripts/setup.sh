#!/bin/sh
set -e

BASE=http://localstack:4566

create_queue() {
  name=$1; shift
  aws sqs create-queue --queue-name "$name" "$@" > /dev/null 2>&1 \
    || aws sqs get-queue-url --queue-name "$name" > /dev/null
  echo "  queue $name ready"
}

create_table() {
  name=$1; shift
  aws dynamodb create-table --table-name "$name" "$@" > /dev/null 2>&1 \
    || aws dynamodb describe-table --table-name "$name" > /dev/null
  echo "  table $name ready"
}

# Wait until LocalStack is accepting AWS API calls (healthcheck only tests the HTTP
# health endpoint, which can pass before SQS/DynamoDB are fully ready).
until aws sqs list-queues > /dev/null 2>&1; do
  sleep 1
done

echo "Provisioning LocalStack resources..."

create_queue events-dlq \
  --attributes VisibilityTimeout=30

create_queue events \
  --attributes VisibilityTimeout=30,MessageRetentionPeriod=1209600,ReceiveMessageWaitTimeSeconds=20

DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$BASE/000000000000/events-dlq" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)

aws sqs set-queue-attributes \
  --queue-url "$BASE/000000000000/events" \
  --attributes "{\"RedrivePolicy\":\"{\\\"maxReceiveCount\\\":\\\"5\\\",\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\"}\"}" \
  > /dev/null

create_table events \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --key-schema AttributeName=id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE

create_table quarantined_events \
  --attribute-definitions AttributeName=event_id,AttributeType=S \
  --key-schema AttributeName=event_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

echo "Setup complete."
