variable "lambda_name" {
    type = string
    default = "totesys_ingestion"
}

variable "db_credentials_oltp" {
  type = string
  default = "db_credentials_oltp"
}

variable "db_credentials_olap" {
  type = string
  default = "db_credentials_olap"
}