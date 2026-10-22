output "integration_name" { value = snowflake_storage_integration.firms_s3_integration.name }
output "stage_name" { value = snowflake_stage.firms_s3_stage.name }
output "storage_aws_iam_user_arn" { value = snowflake_storage_integration.firms_s3_integration.storage_aws_iam_user_arn }
output "storage_aws_external_id" {
  value = coalesce(
    try(snowflake_storage_integration.firms_s3_integration.describe_output[0].storage_aws_external_id[0].value, null),
    try(snowflake_storage_integration.firms_s3_integration.storage_aws_external_id, null),
  )
}
