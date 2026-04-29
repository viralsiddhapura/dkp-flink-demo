remote_state {
  backend = "s3"
  generate = {
    path      = "backend.tf"
    if_exists = "overwrite"
  }
  config = {
    bucket         = get_env("TG_S3_BUCKET")
    key            = "${path_relative_to_include()}/terraform.tfstate"
    region         = get_env("TG_S3_REGION", "us-east-1")
    encrypt        = true
    dynamodb_table = get_env("TG_DDB_LOCK_TABLE", "")
  }
}

generate "provider" {
  path      = "_provider_override.tf"
  if_exists = "overwrite"
  contents  = <<EOF
provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}
EOF
}

inputs = {
  confluent_cloud_api_key    = get_env("CONFLUENT_CLOUD_API_KEY")
  confluent_cloud_api_secret = get_env("CONFLUENT_CLOUD_API_SECRET")
}
