# Lambda 1 - ingestion from OLTP DB to JSON bucket
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




# Lambda 2 - Transformation - JSON bucket to Parquet bucket
resource "aws_lambda_function" "json_to_parquet" {
  function_name = var.lambda_json_to_parquet_name
  role          = aws_iam_role.lambda_json_to_parquet_role.arn
  s3_bucket     = aws_s3_bucket.json_to_parquet_code_bucket.id
  s3_key        = aws_s3_object.lambda_json_to_parquet_code.key
  handler       = "json_to_parquet.lambda_handler"
  runtime       = "python3.11"
  environment {
    variables = {
      "S3_DATA_ID" = aws_s3_bucket.data_bucket.id,
      "S3_DATA_ARN" = aws_s3_bucket.data_bucket.arn,
      "PARQUET_S3_DATA_ID" = aws_s3_bucket.parquet_data_bucket.id,
      "PARQUET_S3_DATA_ARN" = aws_s3_bucket.parquet_data_bucket.arn,
      "J2P_CODE_BUCKET_ID" = aws_s3_bucket.json_to_parquet_code_bucket.id,
      "J2P_CODE_BUCKET_ARN" = aws_s3_bucket.json_to_parquet_code_bucket.arn
    }
  }
  layers        = [aws_lambda_layer_version.temp_boto_layer.arn]
}
#Change Layer

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.json_to_parquet.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.data_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "json_bucket_notification" {
  bucket = aws_s3_bucket.data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.json_to_parquet.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}



# Lambda 3 - Transformation - Parquet bucket to OLAP DB
resource "aws_lambda_function" "parquet_to_OLAP" {
  function_name = var.lambda_OLAP_loader_name
  role          = aws_iam_role.lambda_parquets_to_olap_role.arn
  s3_bucket     = aws_s3_bucket.parquet-to-olap-code_bucket.id
  s3_key        = aws_s3_object.lambda_parquet_to_OLAP_code.key
  handler       = "parquet_to_olap.lambda_handler"
  runtime       = "python3.11"
  environment {
    variables = {
      "PARQUET_S3_DATA_ID" = aws_s3_bucket.parquet_data_bucket.id,
      "PARQUET_S3_DATA_ARN" = aws_s3_bucket.parquet_data_bucket.arn,
      "P2OLAP_CODE_BUCKET_ID" = aws_s3_bucket.parquet-to-olap-code_bucket.id,
      "P2OLAP_CODE_BUCKET_ARN" = aws_s3_bucket.parquet-to-olap-code_bucket.arn
    }
  }
  layers        = [aws_lambda_layer_version.temp_boto_layer.arn]
}
#Change Layer 

resource "aws_lambda_permission" "allow_parquet_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.parquet_to_OLAP.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.parquet_data_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_s3_bucket_notification" "parquet_bucket_notification" {
  bucket = aws_s3_bucket.parquet_data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.parquet_to_OLAP.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_parquet_s3]
}
