terraform {
  required_version = ">= 1.6.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
  backend "s3" {} # configured via Terragrunt remote_state
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

module "flink_compute_pool" {
  source                    = "../../modules/flink-compute-pool"
  display_name              = "dkp-flink-${var.env}"
  cloud                     = var.cloud
  region                    = var.region
  max_cfu                   = var.max_cfu
  environment_id            = var.environment_id
  environment_resource_name = var.environment_resource_name
  flink_rest_resource_id    = var.flink_rest_resource_id
}

module "kafka_topics" {
  source = "../../modules/kafka-topics"

  kafka_cluster_id       = var.kafka_cluster_id
  kafka_rest_endpoint    = var.kafka_rest_endpoint
  kafka_admin_api_key    = var.kafka_admin_api_key
  kafka_admin_api_secret = var.kafka_admin_api_secret

  # Compacted, upsert-capable topics for the SOD path (Issue 1 long-term).
  # Adjust partition counts to match production (these are placeholders).
  topics = {
    "sod_positions" = {
      partitions = 6
      config = {
        "cleanup.policy" = "compact"
        "min.compaction.lag.ms" = "60000"
      }
    }
    "business_date" = {
      partitions = 1
      config = {
        "cleanup.policy" = "compact"
      }
    }
    "transaction_allocation" = {
      partitions = 12
      config = {
        "cleanup.policy" = "delete"
        "retention.ms"   = "604800000" # 7 days
      }
    }
    "intermediate_positions" = {
      partitions = 12
      config = {
        "cleanup.policy" = "compact"
      }
    }
    "live_positions" = {
      partitions = 12
      config = {
        "cleanup.policy" = "compact"
      }
    }
  }
}
