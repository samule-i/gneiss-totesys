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
  force_destroy = true

}


resource "aws_s3_bucket" "json_to_parquet_code_bucket" {
  bucket_prefix = "json-to-parquet-"
}

resource "aws_s3_object" "lambda_json_to_parquet_code" {
  key    = "json_to_parquet_function.zip"
  source = "${path.module}/../json_to_parquet_function.zip"
  bucket = aws_s3_bucket.json_to_parquet_code_bucket.id
}

resource "aws_s3_bucket" "parquet_data_bucket" {
  bucket_prefix = "parquet-"
}

resource "aws_s3_bucket" "parquet-to-olap-code_bucket" {
  bucket_prefix = "olap-loader-"
}

resource "aws_s3_object" "lambda_parquet_to_OLAP_code" {
  key    = "parquet_to_olap_function.zip"
  source = "${path.module}/../parquet_to_olap_function.zip"
  bucket = aws_s3_bucket.parquet-to-olap-code_bucket.id
}


resource "aws_s3_object" "pg8000_layer" {
  key    = "pg8000_layer.zip"
  source = "${path.module}/../aws_assets/pg8000_layer.zip"
  bucket = aws_s3_bucket.code_bucket.id
}


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
