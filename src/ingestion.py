from pg8000.native import Connection, DatabaseError

#########################################
##  To be replaced by GitHub variables ##
#########################################
DB_HOST = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
DB_DB = 'totesys'
DB_USER = 'project_user_2'
DB_PASSWORD = 'gu5WBDQXu8bECfyq'


def ingestion_handler(event, context):
    """
    Connects to database and selects 10 most recent rows
    from sales_order table

    Raises:
        DatabaseError if error occurs
        during connection
    """
    try:
        conn = get_conn()
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


def get_conn():
    """Connect to source database"""
    return (Connection(user=DB_USER, host=DB_HOST,
                       database=DB_DB, password=DB_PASSWORD))


if __name__ == '__main__':
    loaded_data = ingestion_handler()
    print(loaded_data)
