resource "aws_lambda_function" "totesys_ingestion" {
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_role.arn
  s3_bucket     = aws_s3_bucket.code_bucket.id
  s3_key        = aws_s3_object.lambda_code.key
  handler       = "ingestion.lambda_handler"
  runtime       = "python3.11"
  environment {
    variables = {
      "S3_DATA_ID" = aws_s3_bucket.data_bucket.id,
      "S3_DATA_ARN" = aws_s3_bucket.data_bucket.arn,
      "CODE_BUCKET_ID" = aws_s3_bucket.code_bucket.id,
      "CODE_BUCKET_ARN" = aws_s3_bucket.code_bucket.arn
    }
  }
  layers        = ["arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:11",aws_lambda_layer_version.pg8000_layer.arn,aws_lambda_layer_version.temp_boto_layer.arn]
}

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.totesys_ingestion.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.data_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}