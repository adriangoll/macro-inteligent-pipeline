# Infraestructura (Terraform)

Infraestructura AWS como código (placeholders, sin recursos definidos).

```text
infra/
├── main.tf                  # providers, backend, módulos
├── variables.tf             # variables globales
├── outputs.tf               # outputs exportados
├── terraform.tfvars.example # template de variables
├── modules/
│   ├── s3/                  # data lake + athena results
│   ├── glue/                # database + tablas externas
│   ├── athena/              # workgroup
│   └── iam/                 # roles y policies
└── state/backend.tf         # remote backend (S3 + DynamoDB)
```

Flujo previsto (una vez implementado):

```bash
cd infra
terraform init
terraform plan  -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

> El bucket de `tfstate` y la tabla DynamoDB de locking se crean manualmente una
> sola vez (bootstrap) para evitar dependencia circular.
