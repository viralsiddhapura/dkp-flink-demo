output "compute_pool_id" {
  value       = confluent_flink_compute_pool.this.id
  description = "Confluent Flink compute pool id (lfcp-xxxxx)"
}

output "compute_pool_resource_name" {
  value       = confluent_flink_compute_pool.this.resource_name
  description = "CRN of the Flink compute pool"
}

output "runner_service_account_id" {
  value       = confluent_service_account.flink_runner.id
  description = "Service account id used by CI/CD"
}

output "runner_api_key_id" {
  value       = confluent_api_key.flink_runner.id
  description = "API key id of the CI/CD runner (store as CONFLUENT_FLINK_API_KEY in GitHub secrets)"
  sensitive   = true
}

output "runner_api_key_secret" {
  value       = confluent_api_key.flink_runner.secret
  description = "API key secret of the CI/CD runner (store as CONFLUENT_FLINK_API_SECRET in GitHub secrets)"
  sensitive   = true
}
