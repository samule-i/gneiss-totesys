import os
from dotenv import load_dotenv
from pg8000.native import Connection, DatabaseError


load_dotenv()

# Environment variables to be replaced with Secrets
DB_HOST = os.environ['HOST']
DB_DB = os.environ['DATABASE']
DB_USER = 'project_user_2'
DB_PASSWORD = os.environ['PASSWORD']


def get_conn():
    """Connect to source database"""
    return (Connection(user=DB_USER, host=DB_HOST,
                       database=DB_DB, password=DB_PASSWORD))


def main():
    """
    Connects to database and selects 10 most recent rows
    from sales_order table

    Raises:
        InterfaceError or DatabaseError if error occurs
        during connection
    """
    try:
        conn = get_conn()
        print('connected')
        query = ('SELECT * FROM sales_order \
                 ORDER BY sales_order_id DESC LIMIT 10;')
        rows = conn.run(query)
        column_names = [column['name'] for column in conn.columns]
        return column_names, rows
    except (DatabaseError) as pe:
        print(f'pg8000 error: {pe}')
    except Exception as e:
        print(f'Unexpected error: {e}')
    finally:
        conn.close()


if __name__ == '__main__':
    loaded_data = main()
