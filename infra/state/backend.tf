# Configuracion del remote backend (S3 + DynamoDB locking). Placeholder.
# El bucket de estado y la tabla DynamoDB se crean manualmente (bootstrap)
# antes del primer `terraform init`. Ver Architecture.md, seccion 10.

# terraform {
#   backend "s3" {
#     bucket         = "macro-intelligence-tfstate"
#     key            = "prod/terraform.tfstate"
#     region         = "us-east-1"
#     dynamodb_table = "macro-intelligence-tfstate-lock"
#     encrypt        = true
#   }
# }
