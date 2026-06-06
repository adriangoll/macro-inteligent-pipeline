# Variables del modulo glue.

variable "project" {
  description = "Prefijo de naming: {project}-{resource_type}-{env}."
  type        = string
}

variable "env" {
  description = "Entorno: dev o prod."
  type        = string
}

variable "database_name" {
  description = "Nombre de la database del Glue Data Catalog."
  type        = string
  default     = "macro_intelligence"
}

variable "data_lake_bucket_name" {
  description = "Bucket S3 del data lake (capa Gold)."
  type        = string
}
