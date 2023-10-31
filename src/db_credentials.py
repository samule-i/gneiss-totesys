import json
import boto3


class CredentialsException(Exception):
    pass


def get_credentials(credentials_name, logger):
    """Retrieves named database credentials from AWS secretsmanager.

    Args:
        credentials_name (str): name of the db credentials
        logger (Logger): logger object

    Raises:
        CredentialsException: if credentials are not store, wrong
            format, or missing required keys.
        RuntimeError: all other exceptions.

    Returns:
        dict: db credentials with (at least) the following keys:
        "hostname", "port", "database", "username", "password"
    """
    try:
        sm_client = get_secretsmanager_client()

        response = sm_client.get_secret_value(SecretId=credentials_name)

        credentials = json.loads(response["SecretString"])
        if credentials_are_valid(credentials):
            return credentials
    except sm_client.exceptions.ResourceNotFoundException as e:
        logger.error(e)
        raise CredentialsException(
            f"Credentials not found: '{credentials_name}'"
        )
    except json.decoder.JSONDecodeError as e:
        logger.error(e)
        raise CredentialsException(
            "Stored credentials not in proper JSON format."
        )
    except KeyError as e:
        logger.error(e)
        raise CredentialsException(
            "Credentials not valid - are all required fields present?"
        )
    except Exception as e:
        logger.error(e)
        raise RuntimeError


def get_secretsmanager_client():
    return boto3.client(service_name="secretsmanager", region_name="eu-west-2")


def credentials_are_valid(credentials):
    required_keys = ["hostname", "port", "database", "username", "password"]

    for key in required_keys:
        if key not in credentials:
            raise KeyError

    return True
