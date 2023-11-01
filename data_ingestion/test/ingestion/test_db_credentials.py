import boto3
import json
import pytest
import logging

from ingestion.db_credentials import get_credentials, CredentialsException
from moto import mock_secretsmanager


@pytest.fixture
def dummy_log():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.DEBUG)
    return logger


@mock_secretsmanager
def test_get_credentials_returns_stored_json(dummy_log):
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    secret = {
        "hostname": "a",
        "port": 5432,
        "database": "b",
        "username": "c",
        "password": "d",
    }
    sm.create_secret(Name="test_credentials", SecretString=json.dumps(secret))

    response = get_credentials("test_credentials", dummy_log)
    assert response == secret


@mock_secretsmanager
def test_get_credentials_raises_exeption_if_secret_not_found(dummy_log):
    with pytest.raises(CredentialsException):
        get_credentials("not_available", dummy_log)


@mock_secretsmanager
def test_get_credentials_raises_error_if_secret_not_json(dummy_log):
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    secret = "abc123"
    sm.create_secret(Name="test_credentials", SecretString=secret)
    with pytest.raises(CredentialsException):
        get_credentials("test_credentials", dummy_log)


@mock_secretsmanager
def test_get_credentials_raises_error_if_required_fields_missing(dummy_log):
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    secret = {
        "hostname": "a",
        "port": 5432,
        "database": "b",
    }
    sm.create_secret(Name="test_credentials", SecretString=json.dumps(secret))

    with pytest.raises(CredentialsException):
        get_credentials("test_credentials", dummy_log)
