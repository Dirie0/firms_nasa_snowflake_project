output "database" { value = module.foundation.database_name }
output "schema" { value = module.foundation.schema_name }
output "stage" { value = module.s3_integration.stage_name }
output "integration_name" { value = module.s3_integration.integration_name }
output "storage_aws_iam_user_arn" { value = module.s3_integration.storage_aws_iam_user_arn }
output "storage_aws_external_id" { value = module.s3_integration.storage_aws_external_id }
