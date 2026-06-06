# Outputs del modulo glue.

output "glue_database_name" {
  description = "Nombre de la database del Glue Data Catalog."
  value       = aws_glue_catalog_database.macro_intelligence.name
}

output "table_names" {
  description = "Map nombre logico -> nombre fisico de las tablas Gold."
  value = {
    macro_ar_daily   = aws_glue_catalog_table.macro_ar_daily.name
    global_daily     = aws_glue_catalog_table.global_daily.name
    combined_metrics = aws_glue_catalog_table.combined_metrics.name
    llm_insights     = aws_glue_catalog_table.llm_insights.name
  }
}
