PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
PROJECT_NAME = data_ingestion
SHELL := /bin/bash
PROFILE = default
PIP:=pip
MINUMUM_COVERAGE = 90

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

################################################################################################################
# Set Up Dev
dev-setup:
	$(call execute_in_env, $(PIP) install .[dev])
	$(call execute_in_env, $(PIP) install -e .)

# Build / Run
unit-tests: coverage pytest

pytest:
	$(call execute_in_env, pytest -v)

init: create-environment dev-setup

## Run the security test (bandit + safety)
safety:
	$(call execute_in_env, safety check)
bandit:
	$(call execute_in_env, bandit -r -lll src/*/* *c/*/*.py)
flake:
	$(call execute_in_env, flake8 src/ingestion src/json_to_parquet src/parquet_to_olap test)
coverage:
	$(call execute_in_env, coverage run --omit 'venv/*' -m pytest && coverage report -m --fail-under=${MINUMUM_COVERAGE})

## Run all checks
standards: safety bandit flake

terra-destroy:
	terraform -chdir=terraform destroy -auto-approve -var-file=vars.tfvars

terra-init:
	terraform -chdir=terraform init

terra-apply:
	terraform -chdir=terraform apply -auto-approve -var-file=vars.tfvars

terra-reset: terra-destroy	terra-apply