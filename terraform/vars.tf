variable "lambda_name" {
    type = string
    default = "totesys_ingestion"
}

variable "db_credentials_oltp" {
  type = string
  sensitive = true
  nullable=false
}

variable "db_credentials_olap" {
  type = string
  sensitive = true
  nullable=false
}

variable "sns_emails" {
  type = string
  nullable=false
}