import pg8000
import json


def convert_psql_table_to_json(host, database, user, password, table_name):
    try:
        conn = pg8000.connect(user=user, password=password,
                              host=host, database=database)
        cursor = conn.cursor()
        valid_tables = []
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        data = [dict(zip(column_names, row)) for row in rows]
        cursor.close()
        conn.close()
        json_data = json.dumps(data, indent=4)
        return json_data
    except Exception as e:
        return f"Error: {str(e)}"
