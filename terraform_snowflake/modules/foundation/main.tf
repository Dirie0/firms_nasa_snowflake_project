resource "snowflake_database" "firms_database" {
  name = var.database_name
}

resource "snowflake_warehouse" "firms_warehouse" {
  name                = var.warehouse_name
  warehouse_type      = "STANDARD"
  warehouse_size      = var.warehouse_size
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_schema" "firms_schema" {
  database = snowflake_database.firms_database.name
  name     = var.schema_name
  comment  = "All FIRMS tables, stage and file format"
}
