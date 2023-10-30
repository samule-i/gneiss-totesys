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

# If there's an error with importing modules you wrote:
```pip install -e .```

