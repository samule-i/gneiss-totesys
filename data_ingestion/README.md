[![data-ingestion test & deploy](https://github.com/samule-i/gneiss-totesys/actions/workflows/test_deploy.yml/badge.svg)](https://github.com/samule-i/gneiss-totesys/actions/workflows/test_deploy.yml)

## setup
This creates a venv dir, installs dev dependencies and production dependencies
```
make init
```

## postgres set-up
### requires sudo
This will install postgresql and set up a test_user
### Running this more than once will cause issues because the user already exists
`sudo make postgres-install`

## create fake-db
This runs an sql file setting up a basic db
Any added sql files can be added to the Makefile
Currently the path to existing sql is test/sql

`make postgres-database`


## run standards checks
This runs bandit, flake8, safety and coverage
```
make standards
```

## If there's an error with importing your own modules
add:
```
    {
    "python.analysis.extraPaths": [
        "./data_ingestion/src"
    ]
    }
```

to:

`.vscode/settings.json`