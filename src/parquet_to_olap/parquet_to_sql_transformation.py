import pandas as pd
from pg8000.native import Connection, identifier
import logging

log = logging.getLogger("parquet_to_sql_transformation")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
log_fmt = logging.Formatter(
    """%(levelname)s - %(message)s - %(name)s -
    %(module)s/%(funcName)s()"""
)
handler.setFormatter(log_fmt)
log.addHandler(handler)

olap_table_names = {
    "dim_counterparty": "counterparty_id",
    "dim_currency": "currency_id",
    "dim_date": "date_id",
    "dim_design": "design_id",
    "dim_location": "location_id",
    "dim_payment_type": "payment_type_id",
    "dim_staff": "staff_id",
    "dim_transaction": "transaction_id",
    "fact_payment": "payment_record_id",
    "fact_purchase_order": "purchase_record_id",
    "fact_sales_order": "sales_record_id",
}


def parquet_to_sql(dataframe, target_table, conn):
    """
    Writes to database table using upsert method

    Args:
        dataframe: target dataframe
        target_table: table to be written to
        conn: database connection
    """
    olap_table_names = {
        "dim_counterparty": "counterparty_id",
        "dim_currency": "currency_id",
        "dim_date": "date_id",
        "dim_design": "design_id",
        "dim_location": "location_id",
        "dim_payment_type": "payment_type_id",
        "dim_staff": "staff_id",
        "dim_transaction": "transaction_id",
        "fact_payment": "payment_record_id",
        "fact_purchase_order": "purchase_record_id",
        "fact_sales_order": "sales_record_id",
    }

    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("The dataframe input is not of type DataFrame")
    if target_table not in olap_table_names:
        raise ValueError("The target_table input is not a valid table")
    if not isinstance(conn, Connection):
        raise TypeError("The conn input is not a valid pg8000 Connection")
    try:
        target_pkey_column = olap_table_names[target_table]
        query_list = generate_sql_list_for_dataframe(
            dataframe, target_table, target_pkey_column
        )

        for query in query_list:
            conn.run(query)
    except Exception as e:
        log.error(f"{e}")
        raise e


def generate_sql_list_for_dataframe(df, table_name, target_pkey_column):
    column_names = [f"{column}" for column in df.columns]
    column_str = f"({', '.join(column_names)})"

    sql_start = f"INSERT INTO {identifier(table_name)} {column_str} VALUES "

    sql_end = f"on conflict ({target_pkey_column}) "

    update_columns = [
        column for column in column_names if column != target_pkey_column
    ]
    update_columns_str = f"({', '.join(update_columns)})"
    excluded_columns = ["excluded." + column for column in update_columns]
    excluded_str = f"({', '.join(excluded_columns)})"

    sql_end += f"do update set {update_columns_str} = {excluded_str};"

    query_list = []

    for i in range(len(df)):
        row_values = df.iloc[i, :].values.flatten().tolist()
        row_values_str = [
            "'" + f"{str(value)}".replace("'", "''") + "'"
            for value in row_values
        ]
        sql = f'({", ".join(row_values_str)}) '
        query_list.append(sql_start + sql + sql_end)

    return query_list
