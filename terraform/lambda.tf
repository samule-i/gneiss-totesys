##############################################################################
# Lambda 1 - ingestion from OLTP DB to JSON bucket
##############################################################################
resource "aws_lambda_function" "totesys_ingestion" {
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_role.arn
  s3_bucket     = aws_s3_bucket.code_bucket.id
  s3_key        = aws_s3_object.lambda_ingestion_code.key
  handler       = "ingestion.ingestion.lambda_handler"
  runtime       = "python3.11"
  timeout       = 900 # NOTE this must be less than the period defined in the eventbridge rule
  environment {
    variables = {
      "S3_DATA_ID"      = aws_s3_bucket.ingestion_bucket.id,
      "S3_DATA_ARN"     = aws_s3_bucket.ingestion_bucket.arn,
      "CODE_BUCKET_ID"  = aws_s3_bucket.code_bucket.id,
      "CODE_BUCKET_ARN" = aws_s3_bucket.code_bucket.arn
    }
  }
  layers = [
    "arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:11",
    aws_lambda_layer_version.pg8000_layer.arn,
    aws_lambda_layer_version.temp_boto_layer.arn
  ]
}

resource "aws_lambda_permission" "allow_events" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.totesys_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_lambda_invocation_rule.arn
  source_account = data.aws_caller_identity.current.account_id
}

##############################################################################
# Lambda 2 - Transformation - JSON bucket to Parquet bucket
##############################################################################
resource "aws_lambda_function" "json_to_parquet" {
  function_name = var.lambda_json_to_parquet_name
  role          = aws_iam_role.lambda_json_to_parquet_role.arn
  s3_bucket     = aws_s3_bucket.code_bucket.id
  s3_key        = aws_s3_object.lambda_json_to_parquet_code.key
  handler       = "json_to_parquet.json_to_parquet.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60
  environment {
    variables = {
      "S3_DATA_ID"          = aws_s3_bucket.ingestion_bucket.id,
      "S3_DATA_ARN"         = aws_s3_bucket.ingestion_bucket.arn,
      "PARQUET_S3_DATA_ID"  = aws_s3_bucket.transformed_bucket.id,
      "PARQUET_S3_DATA_ARN" = aws_s3_bucket.transformed_bucket.arn,
    }
  }
  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:2"]
}

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.json_to_parquet.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.ingestion_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "json_bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.json_to_parquet.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".json"
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

##############################################################################
# Lambda 3 - Transformation - Parquet bucket to OLAP DB
##############################################################################
resource "aws_lambda_function" "parquet_to_OLAP" {
  function_name = var.lambda_OLAP_loader_name
  role          = aws_iam_role.lambda_parquets_to_olap_role.arn
  s3_bucket     = aws_s3_bucket.code_bucket.id
  s3_key        = aws_s3_object.lambda_parquet_to_OLAP_code.key
  handler       = "parquet_to_olap.parquet_to_olap.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60
  environment {
    variables = {
      "PARQUET_S3_DATA_ID"  = aws_s3_bucket.transformed_bucket.id,
      "PARQUET_S3_DATA_ARN" = aws_s3_bucket.transformed_bucket.arn,
    }
  }
  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:2"]
}

resource "aws_lambda_permission" "allow_parquet_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.parquet_to_OLAP.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.transformed_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "parquet_bucket_notification" {
  bucket = aws_s3_bucket.transformed_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.parquet_to_OLAP.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow_parquet_s3]
}