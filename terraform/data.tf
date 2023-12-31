data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "lambda" {
  type        = "zip"
  excludes = [ "__pycache__", "parquet_to_olap", "json_to_parquet"]
  source_dir = "${path.module}/../src"
  output_path = "${path.module}/../ingestion_function.zip"
}

data "archive_file" "json_to_parquet_lambda" {
  type        = "zip"
  excludes = [ "__pycache__", "ingestion", "parquet_to_olap" ]
  source_dir = "${path.module}/../src"
  output_path = "${path.module}/../json_to_parquet_function.zip"
}

data "archive_file" "parquet_to_OLAP_lambda" {
  type        = "zip"
  excludes = [ "__pycache__", "ingestion", "json_to_parquet"]
  source_dir = "${path.module}/../src"
  output_path = "${path.module}/../parquet_to_olap_function.zip"
}

