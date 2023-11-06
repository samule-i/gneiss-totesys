resource "aws_secretsmanager_secret" "db_creds_oltp" {
  name = "db_credentials_oltp"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_creds_oltp" {
  secret_id     = aws_secretsmanager_secret.db_creds_oltp.id
  secret_string = var.db_credentials_oltp
}

resource "aws_secretsmanager_secret" "db_creds_olap" {
  name = "db_credentials_olap"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_creds_olap" {
  secret_id     = aws_secretsmanager_secret.db_creds_olap.id
  secret_string = var.db_credentials_olap
}