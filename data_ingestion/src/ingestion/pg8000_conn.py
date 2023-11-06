
import pg8000


def get_conn():
    DB_HOST = 'nc-data-eng-totesys-production\
        .chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    DB_DB = 'totesys'
    DB_USER = 'project_user_2'
    DB_PASSWORD = 'gu5WBDQXu8bECfyq'

    conn = (pg8000.connect(user=DB_USER, host=DB_HOST,
                           database=DB_DB, password=DB_PASSWORD))
    return conn


# def main():
#     """
#     Connects to database and selects 10 most recent rows
#     from sales_order table

#     Returns:
#         Column names as list
#         Database rows as list of lists
#     Raises:
#         DatabaseError if error occurs
#         during connection
#     """
#     try:
#         conn = get_conn()
#         print('connected')
#         query = ('SELECT * FROM sales_order \
#                  ORDER BY sales_order_id DESC LIMIT 10;')
#         rows = conn.run(query)
#         column_names = [column['name'] for column in conn.columns]
#         return column_names, rows
#     except (DatabaseError) as pe:
#         print(f'pg8000 error: {pe}')
#     except Exception as e:
#         print(f'Unexpected error: {e}')
#     finally:
#         conn.close()


# if __name__ == '__main__':
#     loaded_data = main()

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
