# Versiones de Terraform/providers y backend de estado remoto (S3 + DynamoDB).
# Ver Architecture.md, seccion 10.
#
# El bucket de estado y la tabla DynamoDB de locking se crean manualmente una
# sola vez (bootstrap) antes del primer `terraform init`, para evitar la
# dependencia circular. No se gestionan con Terraform.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # backend "s3" {
  #   bucket         = "macro-intelligence-tfstate"
  #   key            = "prod/terraform.tfstate"
  #   region         = "us-east-1"
  #   use_lockfile   = true
  #   encrypt        = true
  # }
}
