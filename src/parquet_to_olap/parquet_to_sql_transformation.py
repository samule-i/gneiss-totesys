import pandas as pd
from pg8000.native import Connection, identifier
from utils.custom_log import totesys_logger

log = totesys_logger()

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
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("The dataframe input is not of type DataFrame")
    if target_table not in olap_table_names:
        raise ValueError("The target_table input is not a valid table")
    if not isinstance(conn, Connection):
        raise TypeError("The conn input is not a valid pg8000 Connection")
    try:
        target_pkey_column = olap_table_names[target_table]
        query_str, params_list = generate_sql_list_for_dataframe(
            dataframe, target_table, target_pkey_column
        )

        for params in params_list:
            log.info(f"Executing query with values: {params}")
            conn.run(query_str, **params)

        log.info(f"{len(params_list)} rows upserted for table {target_table}")
    except Exception as e:
        log.error(f"{e}")
        raise e


def generate_sql_list_for_dataframe(df, table_name, target_pkey_column):
    """
    Generates a SQL query and list of params from a Pandas DataFrame.

    Args:
        dataframe: target dataframe
        table_name: The name of the target table in the database.
        target_pkey_column (str): The primary key column.

    Returns:
        tuple: Two elements:
               1st: (str) Parametised SQL INSERT statement.
               2nd: (list of dict) parameter values to match the insert
               statement. This output is intended to be consumed by the
               pg8000.native conn.run method."""

    column_names = [f"{identifier(column)}" for column in df.columns]
    column_str = f"({', '.join(column_names)})"

    update_columns = [
        column for column in column_names if column != target_pkey_column
    ]
    update_columns_str = f"({', '.join(update_columns)})"
    excluded_columns = ["excluded." + column for column in update_columns]
    excluded_str = f"({', '.join(excluded_columns)})"
    column_symbols = [":" + column for column in column_names]

    sql_start = f"INSERT INTO {identifier(table_name)} {column_str} VALUES "
    sql_mid = f'({", ".join(column_symbols)}) '
    sql_end = f"on conflict ({target_pkey_column}) "
    sql_end += f"do update set {update_columns_str} = {excluded_str};"
    final_sql = sql_start + sql_mid + sql_end

    params_list = []

    for i in range(len(df)):
        row_values = df.iloc[i, :].values.flatten().tolist()
        values_dict = dict(zip(column_names, row_values))
        params_list.append(values_dict)

    log.info(f"Query prepared for dataframe: '{final_sql}'")
    log.info(f"Parameters prepared for {len(params_list)} records.")

    return (final_sql, params_list)
