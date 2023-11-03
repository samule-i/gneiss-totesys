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
  topic_arn = aws_sns_topic.user_updates.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.user_updates_queue.arn
}

resource "aws_cloudwatch_metric_alarm" "foobar" {
  alarm_name                = "terraform-test-foobar5"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 2
  metric_name               = "CPUUtilization"
  namespace                 = "AWS/EC2"
  period                    = 120
  statistic                 = "Average"
  threshold                 = 80
  alarm_description         = "This metric monitors ec2 cpu utilization"
  insufficient_data_actions = []
}