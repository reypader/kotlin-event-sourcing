# AWS Coordination (DynamoDB)


```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:Scan",
        "dynamodb:DeleteItem"
      ],
      "Resource": "arn:aws:dynamodb:ap-southeast-1:*:table/cluster_membership"
    }
  ]
}
```

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