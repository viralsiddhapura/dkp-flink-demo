variable "kafka_cluster_id" {
  description = "Kafka cluster id (lkc-xxxxx)"
  type        = string
}

variable "kafka_rest_endpoint" {
  description = "Kafka REST endpoint URL"
  type        = string
}

variable "kafka_admin_api_key" {
  description = "Kafka API key with CloudClusterAdmin or topic-create permissions"
  type        = string
  sensitive   = true
}

variable "kafka_admin_api_secret" {
  description = "Kafka API secret"
  type        = string
  sensitive   = true
}

variable "topics" {
  description = "Map of topic_name => { partitions, config }. Use cleanup.policy=compact for upsert topics."
  type = map(object({
    partitions = number
    config     = map(string)
  }))
  default = {}
}
