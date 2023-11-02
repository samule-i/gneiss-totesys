data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "lambda" {
  type        = "zip"
  excludes = [ "__pycache__" ]|
  source_dir = "${path.module}/../data_ingestion/src/ingestion"
  output_path = "${path.module}/../ingestion_function.zip"
}
