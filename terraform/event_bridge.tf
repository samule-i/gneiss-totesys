resource "aws_cloudwatch_event_rule" "ingestion_lambda_single_invocation_rule" {
  name = "ingestion-lambda-single-invocation-event-rule"
  description = "triggers ingestion lambda at single time"
  schedule_expression = "cron(15 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
  arn = aws_lambda_function.totesys_ingestion.arn
  rule = aws_cloudwatch_event_rule.ingestion_lambda_single_invocation_rule.name
}