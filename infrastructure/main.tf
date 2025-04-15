terraform {
  required_version = ">= 0.12"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "input_bucket" {
  bucket = var.input_bucket_name
  
  tags = {
    Name        = var.input_bucket_name
    Environment = "Test"
  }
}

resource "aws_s3_bucket" "output_bucket" {
  bucket = var.output_bucket_name
  
  tags = {
    Name        = var.output_bucket_name
    Environment = "Test"
  }
}
