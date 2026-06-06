# Outputs exportados del proyecto.

output "data_lake_bucket_name" {
  description = "Nombre del bucket S3 del data lake."
  value       = module.s3.data_lake_bucket_name
}

output "data_lake_bucket_arn" {
  description = "ARN del bucket S3 del data lake."
  value       = module.s3.data_lake_bucket_arn
}

output "athena_results_bucket_name" {
  description = "Nombre del bucket S3 de resultados de Athena."
  value       = module.s3.athena_results_bucket_name
}

output "glue_database_name" {
  description = "Nombre de la database en el Glue Data Catalog."
  value       = module.glue.glue_database_name
}

output "glue_table_names" {
  description = "Map nombre logico -> nombre fisico de las tablas Glue."
  value       = module.glue.table_names
}

output "athena_workgroup_name" {
  description = "Nombre del workgroup de Athena."
  value       = module.athena.workgroup_name
}

output "pipeline_role_arn" {
  description = "ARN del rol IAM del pipeline."
  value       = module.iam.pipeline_role_arn
}

output "pipeline_user_name" {
  description = "Nombre del usuario IAM para dev local (null en prod)."
  value       = module.iam.pipeline_user_name
}
