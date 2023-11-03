resource "aws_cloudwatch_event_rule" "ingestion_lambda_invocation_rule" {
  name                = "ingestion-lambda-invocation-event-rule"
  description         = "triggers ingestion lambda at specified rate"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
  arn  = aws_lambda_function.totesys_ingestion.arn
  rule = aws_cloudwatch_event_rule.ingestion_lambda_invocation_rule.name
}
