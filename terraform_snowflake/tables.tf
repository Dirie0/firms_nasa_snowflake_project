locals {
  tables = [
    {
      name    = "FACT_FIRE_DETECTION"
      comment = "Cleaned fire detections, one row per detection"
      columns = yamldecode(file("${path.root}/schema/fact_fire_detection.yaml"))
    },
    {
      name    = "REJECTED_DETECTIONS"
      comment = "Detections that failed validation, with the reason"
      columns = yamldecode(file("${path.root}/schema/rejected_detections.yaml"))
    },
    {
      name    = "DIM_COUNTRY"
      comment = "Country dimension, keyed on COUNTRY_ISO3"
      columns = yamldecode(file("${path.root}/schema/dim_country.yaml"))
    },
    {
      name    = "DIM_SATELLITE"
      comment = "Satellite dimension, keyed on SATELLITE"
      columns = yamldecode(file("${path.root}/schema/dim_satellite.yaml"))
    },
    {
      name    = "RUN_SUMMARY"
      comment = "Pipeline run summary logs "
      columns = yamldecode(file("${path.root}/schema/run_summary.yaml"))
    },
  ]
}
