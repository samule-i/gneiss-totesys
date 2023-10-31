# Outline

How to set up a pipeline to transfer secret values from GitHub secrets to AWS secretsmanager securely, and with no manual intervention required.

## Requirements

There are two main parts, GitHub and Terraform:

### GitHub

- The JSON snippet containing the credentials must be stored in GitHub secrets encoded as a base64 string (storing raw JSON is strongly discouraged by GitHub, and is difficult to work with).
  - Example json file, before conversion:

```json
{
    "hostname" : "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
    "port" : 5432,
    "database" : "totesys",
    "username" : "project_user_2",
    "password" : "gu5WBDQXu8bECfyq"
}
```

- To convert to base64:

```
base64 local_oltp.json > oltp.base64
```

- Example output:

```
ewogICAgImhvc3RuYW1lIjogIm5jLWRhdGEtZW5nLXRvdGVzeXMtcHJvZHVjdGlvbi5jaHBzY3p0OGgxbnUuZXUtd2VzdC0yLnJkcy5hbWF6b25hd3MuY29tIiwKICAgICJwb3J0IjogNTQzMiwKICAgICJkYXRhYmFzZSI6ICJ0b3Rlc3lzIiwKICAgICJ1c2VybmFtZSI6ICJwcm9qZWN0X3VzZXJfMiIsCiAgICAicGFzc3dvcmQiOiAiZ3U1V0JEUVh1OGJFQ2Z5cSIKfQ==
```

- When storing this in GitHub secrets, it is critical that no linebreaks are inserted by mistake.
- The GitHub actions script must have a step to extract the secret value, decode it from base64 and store it as a local file. This must be before terraform. e.g. :

```
- name: Set db variables
  run: |
 echo ${{ secrets.DB_CREDENTIALS_OLTP }} | base64 -d > secret_oltp.json
- name: Set db variables
  run: |
 echo ${{ secrets.DB_CREDENTIALS_OLAP }} | base64 -d > secret_olap.json
```

### Terraform

- AWS "parameters and secrets" ARN must be entered in the "layers" attribute for the lambda within terraform. This is a fixed ARN provided by AWS. E.g.:

```tf
resource "aws_lambda_function" "secretsmanager_test" {
  s3_bucket     = aws_s3_bucket.code_bucket.id
  s3_key        = aws_s3_object.lambda_code.key
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_role.arn
  layers        = ["arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:11"]
  handler       = "sm-test.lambda_handler"
  runtime       = "python3.11"
}
```

- Each set ot credentials to be stored in secretsmanager (i.e. for OLTP and OLAP) must be defined with a `aws_secretsmanager_secret` and `aws_secretsmanager_secret_version` entry. E.g.:

```tf
resource "aws_secretsmanager_secret" "db_creds_oltp" {
  name = var.db_credentials_oltp
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_creds_oltp" {
  secret_id     = aws_secretsmanager_secret.db_creds_oltp.id
  secret_string = data.local_file.db_credentials_oltp.content
}
```

- Any `aws_secretsmanager_secret` must be stored with `recovery_window_in_days = 0` otherwise it will cause errors destroying and recreating.

  - - The role assigned to the lambda must include a policy for action "secretsmanager:GetSecretValue", and it must reference the arns of the credentials stored in secretsmanager, e.g.:

```tf
data "aws_iam_policy_document" "sm_document" {
  statement {
    actions = ["secretsmanager:GetSecretValue"]

    resources = [
        aws_secretsmanager_secret_version.db_creds_oltp.arn,
        aws_secretsmanager_secret_version.db_creds_olap.arn
    ]
  }
}
```

## Further research

There is a way to cache reads from secretsmanager for improved performance. Non-cached performance is not noticeably bad.
