output "topic_names" {
  value       = [for t in confluent_kafka_topic.this : t.topic_name]
  description = "Created topic names"
}
