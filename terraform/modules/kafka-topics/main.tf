terraform {
  required_version = ">= 1.6.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

# Map of topic_name => { partitions, config }
# Use config.cleanup.policy = "compact" for upsert sources/sinks (Issues 1 & 4).
resource "confluent_kafka_topic" "this" {
  for_each = var.topics

  topic_name       = each.key
  partitions_count = each.value.partitions
  config           = each.value.config

  kafka_cluster {
    id = var.kafka_cluster_id
  }
  rest_endpoint = var.kafka_rest_endpoint

  credentials {
    key    = var.kafka_admin_api_key
    secret = var.kafka_admin_api_secret
  }
}
