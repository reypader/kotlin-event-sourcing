output "table_name" {
  description = "DynamoDB table name"
  value       = aws_dynamodb_table.cluster_membership.name
}

output "table_arn" {
  description = "DynamoDB table ARN"
  value       = aws_dynamodb_table.cluster_membership.arn
}

output "iam_policy_arn" {
  description = "IAM policy ARN for table access"
  value       = aws_iam_policy.dynamodb_access.arn
}