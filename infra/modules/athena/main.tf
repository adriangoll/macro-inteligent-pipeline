# Modulo athena: workgroup dedicado con output location y limite de datos
# escaneados por query. Ver Architecture.md, seccion 10 ("Modulo Athena").

resource "aws_athena_workgroup" "macro_intelligence" {
  name = "${var.project}-workgroup-${var.env}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff_gb * 1024 * 1024 * 1024

    result_configuration {
      output_location = "s3://${var.athena_results_bucket_name}/"
    }
  }
}
