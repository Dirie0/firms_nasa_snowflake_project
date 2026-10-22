output "table_names" { value = [for t in snowflake_table.firms_tables : t.name] }
