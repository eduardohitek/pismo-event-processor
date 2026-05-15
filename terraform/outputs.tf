output "queue_url" {
  value = aws_sqs_queue.events.url
}

output "dlq_url" {
  value = aws_sqs_queue.events_dlq.url
}

output "events_table_name" {
  value = aws_dynamodb_table.events.name
}

output "quarantine_table_name" {
  value = aws_dynamodb_table.quarantined_events.name
}
