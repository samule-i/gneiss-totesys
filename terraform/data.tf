data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "lambda" {
  type        = "zip"
  excludes = [ "__pycache__", "totesys_data_ingestion.egg-info" ]
  source_dir = "${path.module}/../ingestion"
  output_path = "${path.module}/../ingestion_function.zip"
}
