provider "snowflake" {
  organization_name = var.organization_name
  account_name      = var.account_name
  user              = "TERRAFORM_SVC"
  role              = "SYSADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(var.private_key_path)

  preview_features_enabled = [
    "snowflake_storage_integration_resource",
    "snowflake_stage_resource",
    "snowflake_file_format_resource",
    "snowflake_table_resource",
  ]
}


provider "aws" {
  region = var.aws_region
}
