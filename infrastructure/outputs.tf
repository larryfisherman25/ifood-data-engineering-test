output "input_bucket_arn" {
  description = "ARN do bucket de entrada"
  value       = aws_s3_bucket.input_bucket.arn
}

output "output_bucket_arn" {
  description = "ARN do bucket de saída"
  value       = aws_s3_bucket.output_bucket.arn
}
