# Outputs del modulo s3.

output "data_lake_bucket_name" {
  description = "Nombre del bucket del data lake."
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "ARN del bucket del data lake."
  value       = aws_s3_bucket.data_lake.arn
}

output "athena_results_bucket_name" {
  description = "Nombre del bucket de resultados de Athena."
  value       = aws_s3_bucket.athena_results.id
}

output "athena_results_bucket_arn" {
  description = "ARN del bucket de resultados de Athena."
  value       = aws_s3_bucket.athena_results.arn
}
