# Composicion de modulos. Ver Architecture.md, seccion 10 ("main.tf").
# Dependencias: iam y glue dependen de s3; athena usa el bucket de resultados.

module "s3" {
  source = "./modules/s3"

  project                 = var.project
  env                     = var.env
  bronze_glacier_days     = var.bronze_glacier_days
  athena_results_ttl_days = var.athena_results_ttl_days
}

module "glue" {
  source = "./modules/glue"

  project               = var.project
  env                   = var.env
  database_name         = var.glue_database_name
  data_lake_bucket_name = module.s3.data_lake_bucket_name

  depends_on = [module.s3]
}

module "athena" {
  source = "./modules/athena"

  project                    = var.project
  env                        = var.env
  athena_results_bucket_name = module.s3.athena_results_bucket_name
  bytes_scanned_cutoff_gb    = var.bytes_scanned_cutoff_gb
}

module "iam" {
  source = "./modules/iam"

  project                   = var.project
  env                       = var.env
  data_lake_bucket_arn      = module.s3.data_lake_bucket_arn
  athena_results_bucket_arn = module.s3.athena_results_bucket_arn
  glue_database_name        = module.glue.glue_database_name
  athena_workgroup_arn      = module.athena.workgroup_arn
}
