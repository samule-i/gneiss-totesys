##############################################################################
# Policy documents
##############################################################################
data "aws_iam_policy_document" "assume_role_document" {
  statement {

    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }

}

data "aws_iam_policy_document" "sns_publish_document" {
  statement {
    actions = ["sns:Publish"]

    resources = [aws_sns_topic.user_updates.arn]
  }
}

data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = [
      "s3:*Object",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
      "${aws_s3_bucket.transformed_bucket.arn}/*",
      "${aws_s3_bucket.transformed_bucket.arn}"
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

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["*"]
  }
}

##############################################################################
# Policy resources
##############################################################################
resource "aws_iam_policy" "sns_policy" {
  name_prefix = "sns-policy-"
  policy      = data.aws_iam_policy_document.sns_publish_document.json
}

resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-"
  policy      = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_policy" "sm_policy" {
  name_prefix = "sm-policy-"
  policy      = data.aws_iam_policy_document.sm_document.json
}

resource "aws_iam_policy" "cw_policy" {
  name_prefix = "sm-policy-"
  policy = data.aws_iam_policy_document.cw_document.json
}

##############################################################################
# Lambda roles
##############################################################################
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-${var.lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_role_document.json
}

resource "aws_iam_role" "lambda_json_to_parquet_role" {
  name_prefix        = "role-${var.lambda_json_to_parquet_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_role_document.json
}

resource "aws_iam_role" "lambda_parquets_to_olap_role" {
  name_prefix        = "role-${var.lambda_OLAP_loader_name}"
  assume_role_policy = data.aws_iam_policy_document.assume_role_document.json
}

##############################################################################
# Role-policy attachments
##############################################################################

# Ingestion
resource "aws_iam_role_policy_attachment" "ingestion_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_sns_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.sns_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_sm_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.sm_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# JSON to parquet
resource "aws_iam_role_policy_attachment" "json_to_parquet_lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_json_to_parquet_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "json_to_parquet_sns_policy_attachment" {
  role       = aws_iam_role.lambda_json_to_parquet_role.name
  policy_arn = aws_iam_policy.sns_policy.arn
}

resource "aws_iam_role_policy_attachment" "json_to_parquet_cw_policy_attachment" {
  role       = aws_iam_role.lambda_json_to_parquet_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}

# parquet to OLAP
resource "aws_iam_role_policy_attachment" "parquet_to_OLAP_lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_parquets_to_olap_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "parquet_to_OLAP_lambda_sm_policy_attachment" {
  role       = aws_iam_role.lambda_parquets_to_olap_role.name
  policy_arn = aws_iam_policy.sm_policy.arn
}

resource "aws_iam_role_policy_attachment" "parquet_to_OLAP_lambda_cw_policy_attachment" {
  role       = aws_iam_role.lambda_parquets_to_olap_role.name
  policy_arn = aws_iam_policy.cw_policy.arn
}