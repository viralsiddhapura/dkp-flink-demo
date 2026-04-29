terraform {
  required_version = ">= 1.6.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

resource "confluent_flink_compute_pool" "this" {
  display_name = var.display_name
  cloud        = var.cloud
  region       = var.region
  max_cfu      = var.max_cfu
  environment {
    id = var.environment_id
  }
}

# Service account that the CI/CD pipeline uses to submit/stop/resume Flink statements.
resource "confluent_service_account" "flink_runner" {
  display_name = "${var.display_name}-runner"
  description  = "CI/CD service account for Flink statement lifecycle in ${var.display_name}"
}

# FlinkAdmin on the compute pool — required to submit and stop statements.
resource "confluent_role_binding" "flink_runner_admin" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "FlinkAdmin"
  crn_pattern = confluent_flink_compute_pool.this.resource_name
}

# DeveloperRead on the environment so the runner can list/read schemas, topics, etc.
resource "confluent_role_binding" "flink_runner_env_read" {
  principal   = "User:${confluent_service_account.flink_runner.id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = var.environment_resource_name
}

resource "confluent_api_key" "flink_runner" {
  display_name = "${var.display_name}-runner-key"
  description  = "API key for CI/CD Flink runner"
  owner {
    id          = confluent_service_account.flink_runner.id
    api_version = confluent_service_account.flink_runner.api_version
    kind        = confluent_service_account.flink_runner.kind
  }
  managed_resource {
    id          = var.flink_rest_resource_id   # Flink REST endpoint resource id (region-scoped)
    api_version = "fcpm/v2"
    kind        = "Region"
    environment {
      id = var.environment_id
    }
  }
  depends_on = [
    confluent_role_binding.flink_runner_admin,
    confluent_role_binding.flink_runner_env_read,
  ]
}
