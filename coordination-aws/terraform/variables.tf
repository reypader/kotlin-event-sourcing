variable "table_name" {
  description = "Name of the DynamoDB table"
  type        = string
  default     = "event-sourcing-cluster-membership"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}