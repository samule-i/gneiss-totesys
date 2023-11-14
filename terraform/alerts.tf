##############################################################################
# Log groups
##############################################################################
resource "aws_cloudwatch_log_group" "json_to_parquet_logs" {
  name = "/aws/lambda/${aws_lambda_function.json_to_parquet.function_name}"
  depends_on = [aws_lambda_function.json_to_parquet]
}

resource "aws_cloudwatch_log_group" "parquet_to_OLAP_logs" {
  name = "/aws/lambda/${aws_lambda_function.parquet_to_OLAP.function_name}"
  depends_on = [aws_lambda_function.parquet_to_OLAP]
}

resource "aws_cloudwatch_log_group" "ingestion_logs" {
  name = "/aws/lambda/${aws_lambda_function.totesys_ingestion.function_name}"
  depends_on = [aws_lambda_function.totesys_ingestion]
}

##############################################################################
# Alert metrics
##############################################################################
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

resource "aws_cloudwatch_log_metric_filter" "JSON_to_Parquet_Error_Filter" {
  name           = "ErrorFilter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.json_to_parquet_logs.name

  metric_transformation {
    name      = "JSON-to-parquet Lambda"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "Parquet_to_OLAP_Error_Filter" {
  name           = "ErrorFilter"
  pattern        = "ERROR"
  log_group_name = aws_cloudwatch_log_group.parquet_to_OLAP_logs.name

  metric_transformation {
    name      = "Parquet-to-OLAP Lambda"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

##############################################################################
# Alarms
##############################################################################

# Errors
resource "aws_cloudwatch_metric_alarm" "ingestion_alerts" {
  alarm_name                = "Ingestion Error Alert"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "ErrorMetric"
  namespace                 = "CustomLambdaMetrics"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "This metric monitors Errors in the execution of the ingestion application."
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.user_updates.arn]
}

resource "aws_cloudwatch_metric_alarm" "json_to_parquet_alerts" {
  alarm_name                = "Transformation Error Alert"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "JSON-to-parquet Lambda"
  namespace                 = "CustomLambdaMetrics"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "This metric monitors Errors in the execution of the JSON-to-parquet application."
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.user_updates.arn]
}

resource "aws_cloudwatch_metric_alarm" "parquet_to_olap_alerts" {
  alarm_name                = "Loader Error Alert"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "Parquet-to-OLAP Lambda"
  namespace                 = "CustomLambdaMetrics"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "This metric monitors Errors in the execution of the parquet-to-OLAP application."
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.user_updates.arn]
}

# Duration
resource "aws_cloudwatch_metric_alarm" "alert_long_duration_ingestion" {
  alarm_name          = "Ingestion Duration Alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Average"
  threshold           = 45000  # milliseconds
  alarm_description   = "This alarm monitors excessive runtime duration"
  alarm_actions       = [aws_sns_topic.user_updates.arn]
  dimensions = {
    FunctionName = aws_lambda_function.totesys_ingestion.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_long_duration_transformation" {
  alarm_name          = "Transformation Duration Alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Average"
  threshold           = 45000  # milliseconds
  alarm_description   = "This alarm monitors excessive runtime duration"
  alarm_actions       = [aws_sns_topic.user_updates.arn]
  dimensions = {
    FunctionName = aws_lambda_function.json_to_parquet.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "alert_long_duration_loader" {
  alarm_name          = "Loader Duration Alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Average"
  threshold           = 45000  # milliseconds
  alarm_description   = "This alarm monitors excessive runtime duration"
  alarm_actions       = [aws_sns_topic.user_updates.arn]
  dimensions = {
    FunctionName = aws_lambda_function.parquet_to_OLAP.function_name
  }
}

# Concurrent execution
resource "aws_cloudwatch_metric_alarm" "alert_concurrent_execution_ingestion" {
  alarm_name          = "Ingestion Concurrent Execution Alert"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ConcurrentExecutions"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Average"
  threshold           = 1
  alarm_description   = "This alarm monitors concurrent execution for the ingestion lambda."
  alarm_actions       = [aws_sns_topic.user_updates.arn]
  dimensions = {
    FunctionName = aws_lambda_function.totesys_ingestion.function_name
  }
}
##############################################################################
# SNS subscription for alerts
##############################################################################
resource "aws_sns_topic" "user_updates" {
  name = "ErrorAlertMessages"
}

resource "aws_sns_topic_subscription" "user_updates_sqs_target" {
  for_each = toset(split(", ", base64decode(var.sns_emails)))
  topic_arn = aws_sns_topic.user_updates.arn
  protocol  = "email"
  endpoint  = each.value
}
