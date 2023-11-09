import awswrangler as wr
import pandas as pd
from pg8000 import Connection
import logging

log = logging.getLogger('parquet_to_sql_transformation')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
log_fmt = logging.Formatter(
    '''%(levelname)s - %(message)s - %(name)s -
    %(module)s/%(funcName)s()''')
handler.setFormatter(log_fmt)
log.addHandler(handler)


def parquet_to_sql(dataframe, target_table, target_pkey_column, conn):
    """
    Writes to database table using upsert method

    Args:
        dataframe: target dataframe
        target_table: table to be written to
        target_pkey_colum: column in target table that
                           contains primary key
        conn: database connection
    """
    olap_table_names = {'dim_counterparty': 'counterparty_id',
                        'dim_currency': 'currency_id',
                        'dim_date': 'date_id',
                        'dim_design': 'design_id',
                        'dim_location': 'location_id',
                        'dim_payment_type': 'payment_type_id',
                        'dim_staff': 'staff_id',
                        'dim_transaction': 'transaction_id',
                        'fact_payment': 'payment_record_id',
                        'fact_purchase_order': 'purchase_record_id',
                        'fact_sales_order': 'sales_record_id'}

    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError('The dataframe input is not of type DataFrame')
    if target_table not in olap_table_names:
        raise ValueError('The target_table input is not a valid table')
    if target_pkey_column != olap_table_names[target_table]:
        raise ValueError(
            'The target_table does not match the target_pkey_column')
    if not isinstance(conn, Connection):
        raise TypeError('The conn input is not a valid pg8000 Connection')
    try:
        wr.postgresql.to_sql(
            df=dataframe,
            table=target_table,
            schema='public',
            mode='upsert',
            con=conn,
            upsert_conflict_columns=[target_pkey_column]
        )
    except Exception as e:
        log.error(f'{e}')
        raise e
