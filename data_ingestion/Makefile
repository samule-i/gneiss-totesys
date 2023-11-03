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
	sudo apt-get update
	$(call execute_in_env, $(PIP) install .[dev])
	$(call execute_in_env, $(PIP) install -e .)

# Build / Run
unit-tests: coverage pytest


postgres-install:
	sudo apt-get install -y postgresql postgresql-contrib
	service postgresql start
	sudo -u postgres createuser --superuser test_user
	sudo -u postgres createdb test_user
	sudo -u postgres psql -c "ALTER USER test_user PASSWORD 'test_pw';"

postgres-database:
	PGPASSWORD=test_pw psql -U test_user -f ./test/sql/totesys.sql -h localhost

postgres-all: postgres-install postgres-database


pytest:
	$(call execute_in_env, pytest -v)

init: create-environment dev-setup

## Run the security test (bandit + safety)
safety:
	$(call execute_in_env, safety check)
bandit:
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)
flake:
	$(call execute_in_env, flake8  ./src/*/*.py ./test/*/*.py)
coverage:
	$(call execute_in_env, coverage run --omit 'venv/*' -m pytest && coverage report -m --fail-under=${MINUMUM_COVERAGE})

## Run all checks
standards: safety bandit flake