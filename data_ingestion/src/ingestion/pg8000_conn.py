from pg8000 import connect, DatabaseError


def get_conn(credentials):
    """Connect to source database"""
    try:
        DB_HOST = credentials['hostname']
        DB_USER = credentials['username']
        DB_DB = credentials['database']
        DB_PASSWORD = credentials['password']

        return (connect(user=DB_USER, host=DB_HOST,
                        database=DB_DB, password=DB_PASSWORD))
    except DatabaseError as e:
        print('Error connecting to DB')
        raise e
