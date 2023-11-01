data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion.py"
  output_path = "${path.module}/../ingestion_function.zip"
}

# data "archive_file" "temp_boto" {
#   type        = "zip"
#   source_file = "${path.module}/../temp/temp_boto_layer.py"
#   output_path = "${path.module}/../aws_assets/temp_boto_layer.zip"
# }