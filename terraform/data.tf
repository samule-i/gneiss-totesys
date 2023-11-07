data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "lambda" {
  type        = "zip"
  excludes = [ "__pycache__" ]
  source_dir = "${path.module}/../data_ingestion/src/ingestion"
  output_path = "${path.module}/../ingestion_function.zip"
}

data "archive_file" "json_to_parquet_lambda" {
  type        = "zip"
  excludes = [ "__pycache__" ]
  source_dir = "${path.module}/../json_to_parquet/src/json_to_parquet"
  output_path = "${path.module}/../json_to_parquet_function.zip"
}
