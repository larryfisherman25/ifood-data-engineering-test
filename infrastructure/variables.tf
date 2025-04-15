variable "aws_region" {
  description = "Região AWS onde os recursos serão criados"
  type        = string
  default     = "us-east-1"
}

variable "input_bucket_name" {
  description = "Bucket S3 de entrada"
  type        = string
  default     = "yellow-taxi-files-larry-test"
}

variable "output_bucket_name" {
  description = "Bucket S3 de saída"
  type        = string
  default     = "prd-yellow-taxi-table-larry-test"
}
