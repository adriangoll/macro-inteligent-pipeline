# Entry point de Terraform: providers, backend y composicion de modulos.
# Placeholder (scaffolding): sin recursos. Ver Architecture.md, seccion 10.

terraform {
  required_version = ">= 1.5"
  # required_providers { aws = { source = "hashicorp/aws", version = "~> 5.0" } }
}

# provider "aws" {
#   region = var.region
# }

# module "s3"     { source = "./modules/s3" }
# module "iam"    { source = "./modules/iam" }
# module "glue"   { source = "./modules/glue" }
# module "athena" { source = "./modules/athena" }
