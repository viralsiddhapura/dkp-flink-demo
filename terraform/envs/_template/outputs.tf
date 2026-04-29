output "compute_pool_id" {
  value = module.flink_compute_pool.compute_pool_id
}

output "runner_service_account_id" {
  value = module.flink_compute_pool.runner_service_account_id
}

output "runner_api_key_id" {
  value     = module.flink_compute_pool.runner_api_key_id
  sensitive = true
}

output "runner_api_key_secret" {
  value     = module.flink_compute_pool.runner_api_key_secret
  sensitive = true
}

output "topics" {
  value = module.kafka_topics.topic_names
}
