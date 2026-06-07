# Variables del modulo s3.

variable "project" {
  description = "Prefijo de naming: {project}-{resource_type}-{env}."
  type        = string
}

variable "env" {
  description = "Entorno: dev o prod."
  type        = string
}

variable "bronze_glacier_days" {
  description = "Dias antes de archivar Bronze a Glacier."
  type        = number
  default     = 90
}

variable "athena_results_ttl_days" {
  description = "TTL (dias) de resultados de Athena."
  type        = number
  default     = 7
}
