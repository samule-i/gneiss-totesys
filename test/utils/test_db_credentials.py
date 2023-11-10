import boto3
import json
import pytest

from utils.db_credentials import get_credentials, CredentialsException
from moto import mock_secretsmanager


@mock_secretsmanager
def test_get_credentials_returns_stored_json():
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    secret = {
        "hostname": "a",
        "port": 5432,
        "database": "b",
        "username": "c",
        "password": "d",
    }
    sm.create_secret(Name="test_credentials", SecretString=json.dumps(secret))

    response = get_credentials("test_credentials")
    assert response == secret


@mock_secretsmanager
def test_get_credentials_raises_exeption_if_secret_not_found():
    with pytest.raises(CredentialsException):
        get_credentials("not_available")


@mock_secretsmanager
def test_get_credentials_raises_error_if_secret_not_json():
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    secret = "abc123"
    sm.create_secret(Name="test_credentials", SecretString=secret)
    with pytest.raises(CredentialsException):
        get_credentials("test_credentials")


@mock_secretsmanager
def test_get_credentials_raises_error_if_required_fields_missing():
    sm = boto3.client("secretsmanager", region_name="eu-west-2")
    secret = {
        "hostname": "a",
        "port": 5432,
        "database": "b",
    }
    sm.create_secret(Name="test_credentials", SecretString=json.dumps(secret))

    with pytest.raises(CredentialsException):
        get_credentials("test_credentials")


@mock_secretsmanager
def test_get_credentials_raises_runtime_error_for_anything_else():
    with pytest.raises(RuntimeError):
        get_credentials(1)
