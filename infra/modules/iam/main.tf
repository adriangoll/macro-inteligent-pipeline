# Modulo iam: rol y politica de minimo privilegio para el pipeline.
# Ver Architecture.md, seccion 10 ("Modulo IAM").

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name

  glue_catalog_arn  = "arn:aws:glue:${local.region}:${local.account_id}:catalog"
  glue_database_arn = "arn:aws:glue:${local.region}:${local.account_id}:database/${var.glue_database_name}"
  glue_tables_arn   = "arn:aws:glue:${local.region}:${local.account_id}:table/${var.glue_database_name}/*"
}

# Trust policy: el rol puede ser asumido por EC2 (Airflow en EC2) y por
# principals del propio account (usuario IAM de dev local).
data "aws_iam_policy_document" "assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }
  }
}

resource "aws_iam_role" "pipeline_role" {
  name               = "${var.project}-pipeline-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

# Politica de minimo privilegio: S3 (data lake + resultados), Glue (lectura de
# catalogo) y Athena (ejecucion de queries en el workgroup).
data "aws_iam_policy_document" "pipeline" {
  statement {
    sid    = "DataLakeObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
    ]
    resources = ["${var.data_lake_bucket_arn}/*"]
  }

  statement {
    sid       = "DataLakeList"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [var.data_lake_bucket_arn]
  }

  statement {
    sid    = "AthenaResultsObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
    ]
    resources = ["${var.athena_results_bucket_arn}/*"]
  }

  statement {
    sid    = "GlueCatalogRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetPartitions",
    ]
    resources = [
      local.glue_catalog_arn,
      local.glue_database_arn,
      local.glue_tables_arn,
    ]
  }

  statement {
    sid    = "AthenaQueries"
    effect = "Allow"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
    ]
    resources = [var.athena_workgroup_arn]
  }
}

resource "aws_iam_policy" "pipeline" {
  name        = "${var.project}-pipeline-policy-${var.env}"
  description = "Permisos minimos del pipeline (S3 data lake, Glue read, Athena)."
  policy      = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_role_policy_attachment" "pipeline_role" {
  role       = aws_iam_role.pipeline_role.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# Usuario IAM solo para dev local. Las credenciales se generan fuera de
# Terraform y se configuran como variables de entorno.
resource "aws_iam_user" "pipeline_user" {
  count = var.env == "dev" ? 1 : 0
  name  = "${var.project}-pipeline-user-${var.env}"
}

resource "aws_iam_user_policy_attachment" "pipeline_user" {
  count      = var.env == "dev" ? 1 : 0
  user       = aws_iam_user.pipeline_user[0].name
  policy_arn = aws_iam_policy.pipeline.arn
}
