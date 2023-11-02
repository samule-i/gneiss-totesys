import json
import pg8000
import datetime
from decimal import Decimal
import logging
import re
from src.pg8000_conn import get_conn

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj) 
        return super().default(obj)
    
totesys_tables = [
    "address",
    "counterparty",
    "currency",
    "department",
    "design",
    "payment",
    "payment_type",
    "purchase_order",
    "sales_order",
    "staff",
    "transaction",
]

def validate_datetime_format(datetime_str):
    datetime_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}"
    return re.match(datetime_pattern, datetime_str) is not None

def rows_to_json(host, database, user, password, table_name, last_timestamp):
    try:
        if not validate_datetime_format(last_timestamp):
            raise ValueError("last_updated should be in the format 'YYYY-MM-DD HH:MM:SS.SSS'")
    
        elif table_name not in totesys_tables:
            raise ValueError(f"Table '{table_name}' is not a valid totesys table.")
        else:
 
            cursor = get_conn()
            

            query = f"SELECT * FROM {table_name} WHERE CAST(last_updated AS TIMESTAMP) > CAST('{last_timestamp}' AS TIMESTAMP)"
            cursor.execute(query)

            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            record_count = len(rows)
            data = [list(row) for row in rows]

            cursor.close()

            result = {
                "table_name": table_name,
                "column_names": column_names,
                "record_count": record_count,
                "data": data
            }

            json_data = json.dumps(result, indent=4, cls=CustomEncoder)
            return json_data
        
    except pg8000.Error as e:
        logging.error(f"Database Error: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return json.dumps({"error": f"Error: {str(e)}"}, indent=4)

        

# conn = pg8000.connect(user=user, password=password, host=host, database=database)
#             cursor = conn.cursor()
