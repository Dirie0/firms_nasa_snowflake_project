module "foundation" {
  source         = "./modules/foundation"
  database_name  = var.database_name
  schema_name    = var.schema_name
  warehouse_name = var.warehouse_name
  warehouse_size = var.warehouse_size
}

module "tables" {
  source        = "./modules/tables"
  database_name = module.foundation.database_name
  schema_name   = module.foundation.schema_name
  tables        = local.tables # defined in tables.tf
}

module "s3_integration" {
  source         = "./modules/s3_integration"
  database_name  = module.foundation.database_name
  schema_name    = module.foundation.schema_name
  bucket_name    = var.bucket_name
  aws_account_id = var.aws_account_id
  iam_role_name  = var.iam_role_name
}

module "aws_iam" {
  source                = "./modules/aws_iam"
  role_name             = var.iam_role_name
  bucket_name           = var.bucket_name
  snowflake_iam_user    = module.s3_integration.storage_aws_iam_user_arn
}
