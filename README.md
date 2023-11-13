[![data-ingestion test & deploy](https://github.com/samule-i/gneiss-totesys/actions/workflows/test_deploy.yml/badge.svg)](https://github.com/samule-i/gneiss-totesys/actions/workflows/test_deploy.yml)

# gneiss-totesys
Gneiss-totesys generates a Data Warehouse for the company 'Terrific Totes'

It does this by collecting data from their OLTP database and transforming the data into an easily queriable and analysable state on an OLAP database in accordance with a spec that they have provided.

This database provides business insights that are otherwise unavailable and can help inform future business decisions by ensuring that the data is useful and as up to date as possible with their current activity.

TDD is implemented to ensure that this set of applications remains stable and performs to the customers wishes
This set of applications are deployed to Amazon Web Services to provide reliability in uptime and provide a rapid support response from the developers if there is an unexpected change in state anywhere in the pipeline.

## Setup

#### Download the repository
```
git clone https://github.com/samule-i/gneiss-totesys
cd gneiss-totesys
```
#### download requirements & setup environment
```
apt install python3 python-is-python3
make init
```
#### Running standards tests and unit tests
```
make standards && make unit-tests
```

## Deployment
### Setting your environment
Ensure that you have AWS keys available in `~/.aws/credentials`, available when you create a user at [iam/users](https://us-east-1.console.aws.amazon.com/iam/home?region=eu-north-1#/users)
```
[default]
aws_access_key_id = ...
aws_secret_access_key = ...
```
Create the file `./terraform/vars.tfvars`

```tf
db_credentials_olap = "base64 string" // eg. "ZXhhbXBsZSBqYXp6IG11c2ljCg...=="
db_credentials_oltp = "base64 string" // eg. "ZXhhbXBsZSBqYXp6IG11c2ljCg...=="
sns_emails = "base64 string"
lambda_name = "your_ingest_lambda_name"
lambda_json_to_parquet_name = "your_parquet_lambda_name"
lambda_OLAP_loader_name = "your_olap_lambda_name"
```

### email base64 generation
emails should be seperated by `', '` here, it is important because terraform parses emails as a comma seperated list.
```sh
EMAILS="email1@domain.com, email2@domain.com, email3@domain.com,"
echo $EMAILS | base64
```

### credentials base64 generation
Create a json file for both your input & your output database
#### ./example.json
```json
{
    "hostname": "your_postgres_database_host",
    "port": 5432, // Your database port
    "database": "your_postgres_database_name",
    "username": "your_postgres_username",
    "password": "your_postgres_password",
}
```

```sh
cat ./example.json | base64
```

## pre-deployment set-up
### !important
Before deploying this to AWS, you must first manually create a bucket in your account to store terraforms state, create a bucket with a unique name and use this to populate the key `bucket` in the file `./terraform/provider.tf`.

eg:
```
terraform {
  backend "s3" {
    bucket = "YOUR_BACKEND_BUCKET"
    key    = "application.tfstate"
    region = "eu-west-2"
  }
}
```

Having valid AWS credentials & database credentials is necessary for deployment.

### deploying
#### init is require for the first-run only:
```sh
terraform -chdir=terraform init
```
#### to deploy to aws
```sh
terraform -chdir=terraform plan -var-file=vars.tfvars
terraform -chdir=terraform apply -var-file=vars.tfvars
```

# Pipeline
```mermaid
%%{ init: 
    {
        'theme': 'dark'
    }
}%%

graph LR;

subgraph ingestion
SCH([scheduler timer]) -.-> IF{{Ingestion function}}
TDB[RDBMS]--> IF-->S3F[S3 JSON Bucket]
end

subgraph transformation
S3F -.-> TRG([S3 Trigger]) -.-> TF{{Transformation Function}}
S3F-->TF-->S3PQ[S3 Parquet bucket]
end

subgraph extraction
S3PQ -.-> TRGPQ([S3 Trigger]) -.-> EF{{Extraction function}}
S3PQ --> EF-->ADB[OLAP database]
end

subgraph dashboard
ADB --> DSH[Quickswitch Dashboard]
end
```
