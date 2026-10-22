resource "snowflake_table" "firms_tables" {
  for_each = { for t in var.tables : t.name => t }

  database                    = var.database_name
  schema                      = var.schema_name
  name                        = each.value.name
  comment                     = each.value.comment
  cluster_by                  = length(each.value.cluster_by) > 0 ? each.value.cluster_by : null
  data_retention_time_in_days = each.value.data_retention_time_in_days

  dynamic "column" {
    for_each = each.value.columns
    content {
      name     = column.value.name
      type     = column.value.type
      nullable = column.value.nullable
      comment  = column.value.comment
    }
  }
}
