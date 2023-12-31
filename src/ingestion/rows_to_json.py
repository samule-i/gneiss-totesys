import json
import re
from decimal import Decimal
import datetime
from pg8000.native import identifier
from utils.custom_log import totesys_logger

log = totesys_logger()


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
    "payment_type",
    "staff",
    "transaction",
    "sales_order",
    "payment",
    "purchase_order",
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


def rows_to_json(table_name, last_timestamp, conn):
    if not validate_datetime_format(last_timestamp):
        raise ValueError("invalid last_timestamp format")

    if table_name not in totesys_tables:
        raise ValueError(f"Table '{table_name}' is not a valid totesys table.")

    try:
        query = f"""SELECT * FROM {identifier(table_name)} WHERE
            CAST(last_updated AS TIMESTAMP) >
            CAST(:last_timestamp AS TIMESTAMP)"""

        rows = conn.run(query, last_timestamp=last_timestamp)
        column_names = [item["name"] for item in conn.columns]
        record_count = conn.row_count

        result = {
            "table_name": table_name,
            "column_names": column_names,
            "record_count": record_count,
            "data": rows,
        }
        log.info(
            f"JSON prepared for {table_name} with {record_count} records."
        )
        json_data = json.dumps(result, indent=4, cls=CustomEncoder)
        return json_data
    except Exception as e:
        log.error(f"Error: {str(e)}")
        return json.dumps({"error": f"Error: {str(e)}"}, indent=4)
