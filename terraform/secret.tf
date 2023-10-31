resource "aws_secretsmanager_secret" "db_creds_oltp" {
  name = var.db_credentials_oltp
  recovery_window_in_days = 0
}

data "local_file" "db_credentials_oltp" {
  filename = "../secret_oltp.json"
}

resource "aws_secretsmanager_secret_version" "db_creds_oltp" {
  secret_id     = aws_secretsmanager_secret.db_creds_oltp.id
  secret_string = data.local_file.db_credentials_oltp.content
}

resource "aws_secretsmanager_secret" "db_creds_olap" {
  name = var.db_credentials_olap
  recovery_window_in_days = 0
}

data "local_file" "db_credentials_olap" {
  filename = "../secret_olap.json"
}

resource "aws_secretsmanager_secret_version" "db_creds_olap" {
  secret_id     = aws_secretsmanager_secret.db_creds_olap.id
  secret_string = data.local_file.db_credentials_olap.content
}