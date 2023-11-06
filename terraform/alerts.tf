resource "aws_cloudwatch_log_metric_filter" "Error_Filter" {
  name           = "ErrorFilter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.ingestion_logs.name

  metric_transformation {
    name      = "ErrorMetric"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_group" "ingestion_logs" {
  name = "/aws/lambda/${aws_lambda_function.totesys_ingestion.function_name}"
  depends_on = [aws_lambda_function.totesys_ingestion]
}

resource "aws_sns_topic" "user_updates" {
  name = "ErrorAlertMessages"
}

resource "aws_sns_topic_subscription" "user_updates_sqs_target" {
  for_each = toset(split(", ", base64decode(var.sns_emails)))
  topic_arn = aws_sns_topic.user_updates.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_cloudwatch_metric_alarm" "ingestion_alerts" {
  alarm_name                = "Error-Alerts"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 2
  metric_name               = "ErrorMetric"
  namespace                 = "CustomLambdaMetrics"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "This metric monitors Errors in the execution of the ingestion application."
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.user_updates.arn]
}

