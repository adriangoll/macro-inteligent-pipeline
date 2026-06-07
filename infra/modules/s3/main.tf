# Modulo s3: data lake + bucket de resultados de Athena.
# Ver Architecture.md, seccion 10 ("Modulo S3").

locals {
  data_lake_bucket_name      = "${var.project}-data-lake-${var.env}"
  athena_results_bucket_name = "${var.project}-athena-results-${var.env}"
}

# --- Data lake (Bronze / Silver / Gold) ---

resource "aws_s3_bucket" "data_lake" {
  bucket = local.data_lake_bucket_name
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-to-glacier"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = var.bronze_glacier_days
      storage_class = "GLACIER"
    }
  }
}

# --- Resultados de queries Athena ---

resource "aws_s3_bucket" "athena_results" {
  bucket = local.athena_results_bucket_name
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = var.athena_results_ttl_days
    }
  }
}
