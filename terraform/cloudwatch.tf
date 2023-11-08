resource "aws_iam_policy" "ingestion_execution_policy" {
  name = "lambda-execution-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = "logs:CreateLogStream",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action   = "logs:PutLogEvents",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action   = "lambda:InvokeFunction",
        Effect   = "Allow",
        Resource = aws_lambda_function.totesys_ingestion.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_execution_policy_attachment" {
  policy_arn = aws_iam_policy.ingestion_execution_policy.arn
  role       = aws_iam_role.lambda_role.name
}



resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.totesys_ingestion.function_name
  principal     = "events.amazonaws.com"

}






resource "aws_iam_policy" "json_to_parquet_lambda_execution_policy" {
  name = "json-to-parquet-lambda-execution-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = "logs:CreateLogStream",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action   = "logs:PutLogEvents",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action   = "lambda:InvokeFunction",
        Effect   = "Allow",
        Resource = aws_lambda_function.json_to_parquet.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "json_to_parquet_lambda_execution_policy_attachment" {
  policy_arn = aws_iam_policy.json_to_parquet_lambda_execution_policy.arn
  role       = aws_iam_role.lambda_json_to_parquet_role.name
}



resource "aws_lambda_permission" "allow_j2p_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.json_to_parquet.function_name
  principal     = "events.amazonaws.com"
}





resource "aws_iam_policy" "parquet_to_olap_lambda_execution_policy" {
  name = "parquet-to-olap-lambda-execution-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = "logs:CreateLogStream",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action   = "logs:PutLogEvents",
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action   = "lambda:InvokeFunction",
        Effect   = "Allow",
        Resource = aws_lambda_function.parquet_to_OLAP.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "parquet_to_olap_lambda_execution_policy_attachment" {
  policy_arn = aws_iam_policy.parquet_to_olap_lambda_execution_policy.arn
  role       = aws_iam_role.lambda_parquets_to_olap_role.name
}



resource "aws_lambda_permission" "allow_p2olap_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.parquet_to_OLAP.function_name
  principal     = "events.amazonaws.com"

}

  
