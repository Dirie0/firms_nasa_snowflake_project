variable "database_name" { type = string }
variable "schema_name" { type = string }

variable "tables" {
  description = "Tables to create, all in schema_name"
  type = list(object({
    name                        = string
    comment                     = optional(string)
    cluster_by                  = optional(list(string), [])
    data_retention_time_in_days = optional(number)
    columns = list(object({
      name     = string
      type     = string
      nullable = optional(bool, true)
      comment  = optional(string)
    }))
  }))
}
