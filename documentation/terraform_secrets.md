When running terraform locally, you will be required to pass in information that is not kept on the git repository.

This information can be stored in `/terraform/vars.tfvars`
Then simply call plan or apply with the `-var-file=` argument:

`terraform plan -var-file=vars.tfvars`

and the structure should look like:

```
db_credentials_olap = "{ 'hostname': 'example-host', 'port': 5432, 'database': 'example_db', 'username': 'example_user', 'password': 'example-pass', 'schema' : 'example-schema' }"


db_credentials_oltp = "{ 'hostname': 'example-host', 'port': 5432, 'database': 'totesys', 'username': 'example_user', 'password': 'example-pass' }"


sns_emails = ["user1@email.com", "user2@email.com", "user3@email.com"]
```