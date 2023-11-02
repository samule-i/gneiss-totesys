import json
import re
import logging
import pg8000
from decimal import Decimal
import datetime
from src.ingestion import get_conn


class CustomEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling special data types.

    This encoder is used to serialize special data types such as
    datetime, date, and Decimal to their string representations in JSON.
    """

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
    """Validate the format of a datetime string.

    Args:
        datetime_str (str): The datetime string to be validated.

    Returns:
        bool: True if the string matches the expected format, False otherwise.
    """
    datetime_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}"
    return re.match(datetime_pattern, datetime_str) is not None


def rows_to_json(host, database, user, password, table_name, last_timestamp):
    """Convert rows from a PostgreSQL table to JSON.

    Args:
        host (str): The PostgreSQL server host.
        database (str): The name of the PostgreSQL database.
        user (str): The PostgreSQL username.
        password (str): The PostgreSQL password.
        table_name (str): The name of the table to extract data from.
        last_timestamp (str): The timestamp indicating the last update.

    Returns:
        str: JSON representation of the extracted data.
    """
    try:
        if not validate_datetime_format(last_timestamp):
            raise ValueError(
                "invalid last_timestamp format")

        elif table_name not in totesys_tables:
            raise ValueError(
                f"Table '{table_name}' is not a valid totesys table.")
        else:
            conn = get_conn()
            cursor = conn.cursor()

            q1 = f"SELECT * FROM {table_name} WHERE "
            q2 = "CAST(last_updated AS TIMESTAMP) > "
            q3 = f"CAST('{last_timestamp}' AS TIMESTAMP)"
            full_query = q1+q2+q3

            cursor.execute(full_query)

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
