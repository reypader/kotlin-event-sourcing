# AWS Coordination (DynamoDB)


```hcl
# Minimal DynamoDB table for cluster membership
resource "aws_dynamodb_table" "cluster_membership" {
  name     = var.table_name
  hash_key = "nodeId"

  attribute {
    name = "nodeId"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}
```