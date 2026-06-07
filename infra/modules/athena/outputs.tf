# Outputs del modulo athena.

output "workgroup_name" {
  description = "Nombre del workgroup de Athena."
  value       = aws_athena_workgroup.macro_intelligence.name
}

output "workgroup_arn" {
  description = "ARN del workgroup de Athena."
  value       = aws_athena_workgroup.macro_intelligence.arn
}
