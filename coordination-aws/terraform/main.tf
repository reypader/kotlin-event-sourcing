# Minimal DynamoDB table for cluster membership
resource "aws_dynamodb_table" "cluster_membership" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "nodeId"

  attribute {
    name = "nodeId"
    type = "S"
  }

  # TTL for automatic cleanup of stale heartbeats
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Name      = var.table_name
    ManagedBy = "Terraform"
    Purpose   = "Development-Reference"
  }
}

# IAM policy for ECS tasks to access the table
resource "aws_iam_policy" "dynamodb_access" {
  name        = "${var.table_name}-access"
  description = "Allow access to cluster membership table"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "dynamodb:PutItem",
        "dynamodb:DeleteItem",
        "dynamodb:Scan"
      ]
      Resource = aws_dynamodb_table.cluster_membership.arn
    }]
  })
}