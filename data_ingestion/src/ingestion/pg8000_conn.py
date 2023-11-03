from pg8000 import connect

from ingestion.db_credentials import get_credentials


def get_conn(database):
    """Connect to source database"""
    credentials = get_credentials(database)
    DB_HOST = credentials['hostname']
    DB_USER = credentials['username']
    DB_DB = credentials['database']
    DB_PASSWORD = credentials['password']

    return (connect(user=DB_USER, host=DB_HOST,
                    database=DB_DB, password=DB_PASSWORD))
