# IAM user for local development access
resource "aws_iam_user" "local_dev" {
  name = "event-sourcing-local-dev"

  tags = {
    Name      = "event-sourcing-local-dev"
    ManagedBy = "Terraform"
  }
}

# Attach the same DynamoDB access policy
resource "aws_iam_user_policy_attachment" "local_dev_dynamodb" {
  user       = aws_iam_user.local_dev.name
  policy_arn = aws_iam_policy.dynamodb_access.arn
}

# Generate access keys for the user
resource "aws_iam_access_key" "local_dev" {
  user = aws_iam_user.local_dev.name
}

# Output the access keys (use with caution!)
output "local_dev_access_key_id" {
  description = "AWS Access Key ID for local development"
  value       = aws_iam_access_key.local_dev.id
  sensitive   = true
}

output "local_dev_secret_access_key" {
  description = "AWS Secret Access Key for local development"
  value       = aws_iam_access_key.local_dev.secret
  sensitive   = true
}
