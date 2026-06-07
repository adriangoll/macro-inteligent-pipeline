# Outputs del modulo iam.

output "pipeline_role_arn" {
  description = "ARN del rol IAM del pipeline."
  value       = aws_iam_role.pipeline_role.arn
}

output "pipeline_policy_arn" {
  description = "ARN de la politica del pipeline."
  value       = aws_iam_policy.pipeline.arn
}

output "pipeline_user_name" {
  description = "Nombre del usuario IAM de dev local (null si env != dev)."
  value       = var.env == "dev" ? aws_iam_user.pipeline_user[0].name : null
}
