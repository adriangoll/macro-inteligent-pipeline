# Modulo glue: database + tablas externas Gold con partition projection.
# Ver Architecture.md, secciones 6, 7 y 10 ("Modulo Glue").
# Las columnas derivan de las metricas documentadas (secciones 4 y 7).

locals {
  gold_prefix = "s3://${var.data_lake_bucket_name}/gold"

  parquet_input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde         = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  json_input_format  = "org.apache.hadoop.mapred.TextInputFormat"
  json_output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  json_serde         = "org.openx.data.jsonserde.JsonSerDe"

  # Parametros de partition projection comunes (year/month).
  projection_year_month = {
    "projection.enabled"      = "true"
    "projection.year.type"    = "integer"
    "projection.year.range"   = "2024,2030"
    "projection.month.type"   = "integer"
    "projection.month.range"  = "1,12"
    "projection.month.digits" = "2"
  }

  # Columnas por tabla (derivadas de Architecture.md secciones 4 y 7).
  macro_ar_daily_columns = {
    date                 = "date"
    dolar_oficial        = "double"
    dolar_blue           = "double"
    dolar_mep            = "double"
    brecha_cambiaria_pct = "double"
    badlar               = "double"
    leliq_28d            = "double"
    reservas             = "double"
    cpi_arg_mom          = "double"
    cpi_arg_yoy          = "double"
    dolar_blue_pct_1d    = "double"
    dolar_blue_pct_7d    = "double"
    dolar_blue_pct_30d   = "double"
    dolar_blue_ma7       = "double"
    dolar_blue_ma30      = "double"
    dolar_mep_ma7        = "double"
    leliq_real_rate      = "double"
  }

  global_daily_columns = {
    date            = "date"
    btc_usd         = "double"
    eth_usd         = "double"
    dxy_index       = "double"
    cpi_usa         = "double"
    btc_usd_pct_7d  = "double"
    btc_usd_pct_30d = "double"
    btc_usd_ma7     = "double"
    btc_usd_ma30    = "double"
  }

  combined_metrics_columns = {
    date                    = "date"
    dolar_oficial           = "double"
    dolar_blue              = "double"
    dolar_mep               = "double"
    brecha_cambiaria_pct    = "double"
    badlar                  = "double"
    leliq_28d               = "double"
    reservas                = "double"
    cpi_arg_mom             = "double"
    cpi_arg_yoy             = "double"
    btc_usd                 = "double"
    eth_usd                 = "double"
    dxy_index               = "double"
    cpi_usa                 = "double"
    dolar_blue_ma7          = "double"
    dolar_blue_ma30         = "double"
    btc_usd_ma7             = "double"
    btc_usd_ma30            = "double"
    btc_ars_blue            = "double"
    btc_ars_blue_ma7        = "double"
    leliq_real_rate         = "double"
    dxy_vs_brecha_corr_30d  = "double"
    cpi_ar_vs_cpi_usa_ratio = "double"
    is_interpolated         = "boolean"
    dolar_blue_spike        = "boolean"
    brecha_over_100pct      = "boolean"
    btc_drop_7d             = "boolean"
    btc_rally_7d            = "boolean"
    cpi_above_10pct         = "boolean"
  }

  llm_insights_columns = {
    generated_at = "string"
    week_label   = "string"
    summary_text = "string"
    insights     = "array<struct<metric:string,observation:string,magnitude:string>>"
  }
}

resource "aws_glue_catalog_database" "macro_intelligence" {
  name = var.database_name
}

# --- macro_ar_daily (Parquet, year/month) ---
resource "aws_glue_catalog_table" "macro_ar_daily" {
  name          = "macro_ar_daily"
  database_name = aws_glue_catalog_database.macro_intelligence.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(local.projection_year_month, {
    "classification"            = "parquet"
    "storage.location.template" = "${local.gold_prefix}/macro_ar_daily/year=$${year}/month=$${month}"
  })

  partition_keys {
    name = "year"
    type = "int"
  }
  partition_keys {
    name = "month"
    type = "int"
  }

  storage_descriptor {
    location      = "${local.gold_prefix}/macro_ar_daily/"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.macro_ar_daily_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

# --- global_daily (Parquet, year/month) ---
resource "aws_glue_catalog_table" "global_daily" {
  name          = "global_daily"
  database_name = aws_glue_catalog_database.macro_intelligence.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(local.projection_year_month, {
    "classification"            = "parquet"
    "storage.location.template" = "${local.gold_prefix}/global_daily/year=$${year}/month=$${month}"
  })

  partition_keys {
    name = "year"
    type = "int"
  }
  partition_keys {
    name = "month"
    type = "int"
  }

  storage_descriptor {
    location      = "${local.gold_prefix}/global_daily/"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.global_daily_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

# --- combined_metrics (Parquet, year/month) ---
resource "aws_glue_catalog_table" "combined_metrics" {
  name          = "combined_metrics"
  database_name = aws_glue_catalog_database.macro_intelligence.name
  table_type    = "EXTERNAL_TABLE"

  parameters = merge(local.projection_year_month, {
    "classification"            = "parquet"
    "storage.location.template" = "${local.gold_prefix}/combined_metrics/year=$${year}/month=$${month}"
  })

  partition_keys {
    name = "year"
    type = "int"
  }
  partition_keys {
    name = "month"
    type = "int"
  }

  storage_descriptor {
    location      = "${local.gold_prefix}/combined_metrics/"
    input_format  = local.parquet_input_format
    output_format = local.parquet_output_format

    ser_de_info {
      serialization_library = local.parquet_serde
    }

    dynamic "columns" {
      for_each = local.combined_metrics_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

# --- llm_insights (JSON, year/month/week) ---
resource "aws_glue_catalog_table" "llm_insights" {
  name          = "llm_insights"
  database_name = aws_glue_catalog_database.macro_intelligence.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"            = "json"
    "projection.enabled"        = "true"
    "projection.year.type"      = "integer"
    "projection.year.range"     = "2024,2030"
    "projection.month.type"     = "integer"
    "projection.month.range"    = "1,12"
    "projection.month.digits"   = "2"
    "projection.week.type"      = "integer"
    "projection.week.range"     = "1,53"
    "projection.week.digits"    = "2"
    "storage.location.template" = "${local.gold_prefix}/llm_insights/year=$${year}/month=$${month}/week=$${week}"
  }

  partition_keys {
    name = "year"
    type = "int"
  }
  partition_keys {
    name = "month"
    type = "int"
  }
  partition_keys {
    name = "week"
    type = "int"
  }

  storage_descriptor {
    location      = "${local.gold_prefix}/llm_insights/"
    input_format  = local.json_input_format
    output_format = local.json_output_format

    ser_de_info {
      serialization_library = local.json_serde
    }

    dynamic "columns" {
      for_each = local.llm_insights_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}
