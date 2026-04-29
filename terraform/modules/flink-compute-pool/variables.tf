variable "display_name" {
  description = "Display name of the Flink compute pool (e.g., dkp-flink-prod)"
  type        = string
}

variable "cloud" {
  description = "Cloud provider — AWS, GCP, or AZURE"
  type        = string
}

variable "region" {
  description = "Cloud region (e.g., us-east-1)"
  type        = string
}

variable "max_cfu" {
  description = "Maximum CFUs (Confluent Flink Units). Start at 5 for non-prod, 10+ for prod."
  type        = number
  default     = 5
}

variable "environment_id" {
  description = "Confluent Cloud environment ID (env-xxxxx)"
  type        = string
}

variable "environment_resource_name" {
  description = "CRN of the environment (e.g., crn://confluent.cloud/organization=.../environment=env-xxxxx)"
  type        = string
}

variable "flink_rest_resource_id" {
  description = "Flink REST endpoint resource id for the region (Confluent docs: fcpm/v2 Region id)"
  type        = string
}
