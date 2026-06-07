# Variables del modulo athena.

variable "project" {
  description = "Prefijo de naming: {project}-{resource_type}-{env}."
  type        = string
}

variable "env" {
  description = "Entorno: dev o prod."
  type        = string
}

variable "athena_results_bucket_name" {
  description = "Bucket S3 donde Athena escribe los resultados de queries."
  type        = string
}

variable "bytes_scanned_cutoff_gb" {
  description = "Limite de GB escaneados por query (proteccion de costos)."
  type        = number
  default     = 10
}
