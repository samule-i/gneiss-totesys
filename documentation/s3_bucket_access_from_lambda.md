#### short answer
```
import os

data_bucket_arn = os.environ['S3_DATA']
```

[AWS docs](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html)

### Editing available environment variables
see `terraform\lambda.tf`
and edit
```
  environment {
    variables = {
      "NEW_KEY" = 'string',
      "OTHER_KEY" = or.resource.arn
    }
  }
```