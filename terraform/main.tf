terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = var.region
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  endpoints {
    sqs      = "http://localstack:4566"
    dynamodb = "http://localstack:4566"
  }
}

resource "aws_sqs_queue" "events_dlq" {
  name = "events-dlq"
}

resource "aws_sqs_queue" "events" {
  name                       = "events"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 1209600
  receive_wait_time_seconds  = 20

  redrive_policy = jsonencode({
    maxReceiveCount     = 5
    deadLetterTargetArn = aws_sqs_queue.events_dlq.arn
  })
}

resource "aws_dynamodb_table" "events" {
  name         = "events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"
}

resource "aws_dynamodb_table" "quarantined_events" {
  name         = "quarantined_events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }
}
