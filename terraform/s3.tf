resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "totesys-ingress-"
}

resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "totesys-transform-"
}

resource "aws_s3_object" "pg8000_layer" {
  key    = "pg8000_layer.zip"
  source = "${path.module}/../pg8000_layer.zip"
  bucket = aws_s3_bucket.code_bucket.id
}