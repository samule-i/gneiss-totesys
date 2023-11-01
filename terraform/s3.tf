resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "totesys-ingress-"
}
resource "aws_s3_object" "lambda_code" {
  key    = "ingestion_function.zip"
  source = "${path.module}/../ingestion_function.zip"
  bucket = aws_s3_bucket.code_bucket.id
}

resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "totesys-transform-"
}

resource "aws_s3_object" "pg8000_layer" {
  key    = "pg8000_layer.zip"
  source = "${path.module}/../aws_assets/pg8000_layer.zip"
  bucket = aws_s3_bucket.code_bucket.id
}

resource "aws_lambda_layer_version" "pg8000_layer" {
  #filename   = "${path.module}/../aws_assets/pg8000_layer.zip"
  s3_key = "pg8000_layer.zip"
  s3_bucket = aws_s3_bucket.code_bucket.id
  layer_name = "pg8000_layer"

  compatible_runtimes = ["python3.11"]
}

# resource "aws_s3_object" "temp_boto_layer" {
#   key    = "pg8000_layer.zip"
#   source = "${path.module}/../aws_assets/pg8000_layer.zip"
#   bucket = aws_s3_bucket.code_bucket.id
# }