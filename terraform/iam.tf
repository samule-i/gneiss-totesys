
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-${var.lambda_name}"
  assume_role_policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
      })
       inline_policy {
    name = "sns_publish_policy"
    policy = jsonencode({
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": "sns:Publish",
          "Resource": aws_sns_topic.user_updates.arn
        }
      ]
    })
  }
}



resource "aws_iam_role" "lambda_json_to_parquet_role" {
  name_prefix        = "role-${var.lambda_json_to_parquet_name}"
  assume_role_policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
      })
       inline_policy {
    name = "sns_publish_policy"
    policy = jsonencode({
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": "sns:Publish",
          "Resource": aws_sns_topic.user_updates.arn
        }
      ]
    })
  }
}

resource "aws_iam_role" "lambda_parquets_to_olap_role" {
  name_prefix        = "role-${var.lambda_OLAP_loader_name}"
  assume_role_policy = jsonencode({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
      })
       inline_policy {
    name = "sns_publish_policy"
    policy = jsonencode({
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": "sns:Publish",
          "Resource": aws_sns_topic.user_updates.arn
        }
      ]
    })
  }
}


data "aws_iam_policy_document" "s3_document" {
  statement {
    effect = "Allow"
    actions = ["s3:*Object",
               "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.data_bucket.arn}/*",
      "${aws_s3_bucket.json_to_parquet_code_bucket.arn}/*",
      "${aws_s3_bucket.parquet_data_bucket.arn}/*",
      "${aws_s3_bucket.parquet-to-olap-code_bucket.arn}/*"
    ]
  }
}

data "aws_iam_policy_document" "sm_document" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]

    resources = [
        aws_secretsmanager_secret_version.db_creds_oltp.arn,
        aws_secretsmanager_secret_version.db_creds_olap.arn
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-${var.lambda_name}"
  policy      = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "sm_policy" {
  name_prefix = "sm-policy-${var.lambda_name}"
  policy      = data.aws_iam_policy_document.sm_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "parquet_to_OLAP_lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_parquets_to_olap_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "json_to_parquet_lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_json_to_parquet_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}
resource "aws_iam_role_policy_attachment" "lambda_sm_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.sm_policy.arn
}
