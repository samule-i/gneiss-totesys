from pg8000.native import identifier

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


def get_table_extract_sql(table_name):
    """Generate query string to retrieve delta of records since
       the previous timestamp.
       NOTE - this is a paremtised query string!

    Args:
        table_name (str): name of table to query

    Raises:
        ValueError: if table_name is not in totesys_tables.

    Returns:
        str: SQL string in the following format:
        "SELECT * FROM table_name WHERE last_updated > :previous_timestamp;"
    """
    if table_name not in totesys_tables:
        raise ValueError(f"Table '{table_name}' is not a valid totesys table.")

    sql = (
        f"SELECT * FROM {identifier(table_name)} "
        f"WHERE last_updated > :previous_timestamp;"
    )
    return sql
