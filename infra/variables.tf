# Variables globales del proyecto. Ver Architecture.md, seccion 10.

variable "env" {
  description = "Entorno de despliegue: dev o prod."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env debe ser 'dev' o 'prod'."
  }
}

variable "region" {
  description = "Region AWS."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefijo para naming de recursos: {project}-{resource_type}-{env}."
  type        = string
  default     = "macro-intelligence"
}

variable "glue_database_name" {
  description = "Nombre de la database en el Glue Data Catalog."
  type        = string
  default     = "macro_intelligence"
}

variable "bronze_glacier_days" {
  description = "Dias antes de archivar la capa Bronze a Glacier."
  type        = number
  default     = 90
}

variable "athena_results_ttl_days" {
  description = "TTL (dias) para limpiar resultados de queries Athena."
  type        = number
  default     = 7
}

variable "bytes_scanned_cutoff_gb" {
  description = "Limite de GB escaneados por query en el workgroup de Athena."
  type        = number
  default     = 10
}
