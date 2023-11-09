variable "lambda_name" {
    type = string
    default = "totesys_ingestion"
}

variable "lambda_json_to_parquet_name" {
    type = string
    default = "json_to_parquet"
}

variable "lambda_OLAP_loader_name" {
    type = string
    default = "OLAP_loader"
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
