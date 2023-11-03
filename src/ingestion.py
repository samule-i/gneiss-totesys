from pg8000 import connect, DatabaseError
import boto3
import json
import logging


class CredentialsException(Exception):
    pass


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def ingestion_handler(event, context):
    """
    Connects to database and selects 10 most recent rows
    from sales_order table

    Returns:
        column_names: list of DB table column names
        rows: list of lists of DB table row values
    Raises:
        DatabaseError if error occurs
        during connection
    """
    try:
        conn = get_conn()
        cursor = conn.cursor()
        query = ('SELECT * FROM sales_order \
                 ORDER BY sales_order_id DESC LIMIT 10;')
        rows = cursor.execute(query)
        column_names = [column['name'] for column in conn.columns]
        return column_names, rows
    except (DatabaseError) as pe:
        print(f'pg8000 error: {pe}')
    except Exception as e:
        print(f'Unexpected error: {e}')
    finally:
        cursor.close()
        conn.close()


def get_conn():
    """Connect to client OLTP database"""
    credentials = get_credentials('db_credentials_oltp', logger)
    DB_HOST = credentials['hostname']
    DB_DB = credentials['database']
    DB_USER = credentials['username']
    DB_PASSWORD = credentials['password']

    conn = (connect(user=DB_USER, host=DB_HOST,
                    database=DB_DB, password=DB_PASSWORD))
    return conn


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


if __name__ == '__main__':
    loaded_data = ingestion_handler()
