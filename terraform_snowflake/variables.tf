variable "organization_name" {
  type        = string
  description = "Snowflake organization name (first part of the account identifier)"
}
variable "account_name" {
  type        = string
  description = "Snowflake account name within the organization"
}
variable "private_key_path" {
  type        = string
  description = "Path to the PKCS#8 private key (.p8) for the TERRAFORM_SVC user"
}
variable "database_name" {
  type        = string
  default     = "FIRMS_DB"
  description = "Database to create"
}
variable "warehouse_name" {
  type        = string
  default     = "FIRMS_WH"
  description = "Warehouse to create"
}
variable "warehouse_size" {
  type        = string
  default     = "XSMALL"
  description = "Warehouse size"
}
variable "schema_name" {
  type        = string
  default     = "FIRMS"
  description = "Single schema that holds all tables, the stage and the file format"
}

variable "aws_region" {
  type        = string
  default     = "eu-west-2"
  description = "AWS region for the IAM provider"
}
variable "aws_account_id" {
  type        = string
  description = "12-digit AWS account id that owns the bucket"
}
variable "bucket_name" {
  type        = string
  description = "S3 bucket Snowflake reads from"
}
variable "iam_role_name" {
  type        = string
  default     = "snowflake-firms-s3-role"
  description = "Name of the IAM role Snowflake assumes"
}
