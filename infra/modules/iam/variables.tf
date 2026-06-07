# Variables del modulo iam.

variable "project" {
  description = "Prefijo de naming: {project}-{resource_type}-{env}."
  type        = string
}

variable "env" {
  description = "Entorno: dev o prod."
  type        = string
}

variable "data_lake_bucket_arn" {
  description = "ARN del bucket del data lake."
  type        = string
}

variable "athena_results_bucket_arn" {
  description = "ARN del bucket de resultados de Athena."
  type        = string
}

variable "glue_database_name" {
  description = "Nombre de la database del Glue Data Catalog."
  type        = string
}

variable "athena_workgroup_arn" {
  description = "ARN del workgroup de Athena."
  type        = string
}
