# Variables globales del proyecto (placeholder).

variable "env" {
  description = "Entorno: dev o prod."
  type        = string
  default     = "dev"
}

variable "region" {
  description = "Region AWS."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefijo para naming de recursos."
  type        = string
  default     = "macro-intelligence"
}

variable "bronze_glacier_days" {
  description = "Dias para archivar Bronze a Glacier."
  type        = number
  default     = 90
}

variable "athena_results_ttl_days" {
  description = "TTL de resultados Athena."
  type        = number
  default     = 7
}

variable "bytes_scanned_cutoff_gb" {
  description = "Limite de GB escaneados por query en Athena."
  type        = number
  default     = 10
}
