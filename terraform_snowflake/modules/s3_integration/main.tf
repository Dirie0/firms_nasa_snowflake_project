locals {
  role_arn = "arn:aws:iam::${var.aws_account_id}:role/${var.iam_role_name}"
}

resource "snowflake_storage_integration" "firms_s3_integration" {
  name                      = "FIRMS_S3_INT"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = local.role_arn
  storage_allowed_locations = ["s3://${var.bucket_name}/"]
}

resource "snowflake_file_format" "firms_csv_format" {
  database                     = var.database_name
  schema                       = var.schema_name
  name                         = "FIRMS_CSV"
  format_type                  = "CSV"
  skip_header                  = 1
  field_delimiter              = ","
  field_optionally_enclosed_by = "\""
  null_if                      = [""]
}

resource "snowflake_stage" "firms_s3_stage" {
  database            = var.database_name
  schema              = var.schema_name
  name                = "FIRMS_S3_STAGE"
  url                 = "s3://${var.bucket_name}/"
  storage_integration = snowflake_storage_integration.firms_s3_integration.name
}
