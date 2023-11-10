##############################################################################
# Buckets
##############################################################################
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "gneiss-totesys-code-"
  force_destroy = true
}

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = "gneiss-totesys-ingestion-"
  force_destroy = true
}

resource "aws_s3_bucket" "transformed_bucket" {
  bucket_prefix = "gneiss-totesys-transformed-"
  force_destroy = true
}

##############################################################################
# Lambda code
##############################################################################
resource "aws_s3_object" "lambda_ingestion_code" {
  key    = "ingestion_function.zip"
  source = "${path.module}/../ingestion_function.zip"
  bucket = aws_s3_bucket.code_bucket.id
}

resource "aws_s3_object" "lambda_json_to_parquet_code" {
  key    = "json_to_parquet_function.zip"
  source = "${path.module}/../json_to_parquet_function.zip"
  bucket = aws_s3_bucket.code_bucket.id
}

resource "aws_s3_object" "lambda_parquet_to_OLAP_code" {
  key    = "parquet_to_olap_function.zip"
  source = "${path.module}/../parquet_to_olap_function.zip"
  bucket = aws_s3_bucket.code_bucket.id
}

##############################################################################
# Lambda layers
##############################################################################
resource "aws_lambda_layer_version" "pg8000_layer" {
  filename   = "${path.module}/../aws_assets/pg8000_layer.zip"
  layer_name = "pg8000_layer"

  compatible_runtimes = ["python3.11"]
}

resource "aws_lambda_layer_version" "temp_boto_layer" {
  filename   = "${path.module}/../aws_assets/boto3_layer.zip"
  layer_name = "boto3_layer"

  compatible_runtimes = ["python3.11"]
}
