[![data-ingestion test & deploy](https://github.com/samule-i/gneiss-totesys/actions/workflows/test_deploy.yml/badge.svg)](https://github.com/samule-i/gneiss-totesys/actions/workflows/test_deploy.yml)

## setup
This creates a venv dir, installs dev dependencies and production dependencies
```
make init
```

## run standards checks
This runs bandit, flake8, safety and coverage
```
make standards
```

## If there's an error with importing your own modules
add:
    ```{
    "python.analysis.extraPaths": [
        "./data_ingestion/src"
    ]
}```

to:

`.vscode/settings.json`