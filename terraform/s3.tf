resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "totesys-ingress-"
}

resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "totesys-transform-"
}
