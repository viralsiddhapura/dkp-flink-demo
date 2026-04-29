include "root" {
  path = find_in_parent_folders()
}

terraform {
  source = "../..//terraform/envs/_template"
}

inputs = {
  env                       = "dev"
  cloud                     = "AWS"
  region                    = "us-east-1"
  max_cfu                   = 5
  environment_id            = get_env("CONFLUENT_ENV_ID_DEV")
  environment_resource_name = get_env("CONFLUENT_ENV_CRN_DEV")
  flink_rest_resource_id    = get_env("CONFLUENT_FLINK_REGION_ID_DEV")
  kafka_cluster_id          = get_env("CONFLUENT_KAFKA_CLUSTER_ID_DEV")
  kafka_rest_endpoint       = get_env("CONFLUENT_KAFKA_REST_ENDPOINT_DEV")
  kafka_admin_api_key       = get_env("CONFLUENT_KAFKA_ADMIN_API_KEY_DEV")
  kafka_admin_api_secret    = get_env("CONFLUENT_KAFKA_ADMIN_API_SECRET_DEV")
}
