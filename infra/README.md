# Infraestructura (Terraform)

Infraestructura AWS como código para el Macro Intelligence Pipeline
(S3 data lake, Glue Data Catalog, Athena, IAM). Ver `Architecture.md` sección 10.

```text
infra/
├── terraform.tf             # versiones + backend remoto (S3 + DynamoDB)
├── providers.tf             # provider AWS (~> 5.0)
├── main.tf                  # composición de módulos
├── variables.tf             # variables globales
├── outputs.tf               # outputs exportados
├── terraform.tfvars.example # template de variables
└── modules/
    ├── s3/                  # data lake + bucket de resultados de Athena
    ├── glue/                # database + 4 tablas externas Gold
    ├── athena/              # workgroup dedicado
    └── iam/                 # rol, policy de mínimo privilegio y user (dev)
```

## Bootstrap del backend (una sola vez)

El bucket de `tfstate` y la tabla DynamoDB de locking se crean manualmente
**antes** del primer `terraform init`, para evitar la dependencia circular
(no se gestionan con Terraform):

```bash
aws s3api create-bucket --bucket macro-intelligence-tfstate --region us-east-1
aws s3api put-bucket-versioning --bucket macro-intelligence-tfstate \
  --versioning-configuration Status=Enabled
aws dynamodb create-table --table-name macro-intelligence-tfstate-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST --region us-east-1
```

## Flujo

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # ajustar valores
terraform init
terraform plan  -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

Convención de naming: `{project}-{resource_type}-{env}`
(ej. `macro-intelligence-data-lake-prod`).
