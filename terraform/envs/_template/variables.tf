variable "env" {
  type        = string
  description = "Environment name: dev|staging|prod"
}

variable "cloud" {
  type    = string
  default = "AWS"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "max_cfu" {
  type    = number
  default = 5
}

variable "environment_id" {
  type        = string
  description = "Confluent Cloud environment id (env-xxxxx)"
}

variable "environment_resource_name" {
  type        = string
  description = "Environment CRN"
}

variable "flink_rest_resource_id" {
  type        = string
  description = "Flink REST endpoint resource id for the region"
}

variable "kafka_cluster_id" {
  type = string
}

variable "kafka_rest_endpoint" {
  type = string
}

variable "kafka_admin_api_key" {
  type      = string
  sensitive = true
}

variable "kafka_admin_api_secret" {
  type      = string
  sensitive = true
}

variable "confluent_cloud_api_key" {
  type        = string
  sensitive   = true
  description = "Confluent Cloud API key with OrganizationAdmin (bootstrap)"
}

variable "confluent_cloud_api_secret" {
  type      = string
  sensitive = true
}
