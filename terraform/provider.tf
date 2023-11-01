provider "aws" {
  region  = "eu-west-2"
}

terraform {
  backend "s3" {
    bucket = "gneiss-totesys-backend"
    key    = "application.tfstate"
    region = "eu-west-2"
  }
}