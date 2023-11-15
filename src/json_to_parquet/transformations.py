import pandas as pd
import json
import logging

log = logging.getLogger('json to dataframe tranformation')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
log_fmt = logging.Formatter(
    """%(levelname)s - %(message)s - %(name)s -
    %(module)s/%(funcName)s()"""
)
handler.setFormatter(log_fmt)
log.addHandler(handler)

"""
Functions Included:
transform_sales_order,
transform_address,
transform_design,
transform_purchase_order,
transform_payment,
transform_transaction,
transform_payment_type.

Functions transform JSON data into a Pandas DataFrame for a specified table.

Parameters:
- table_data : JSON data containing information about the table.

Returns:
- pd.DataFrame: data in a DataFrame, matching star schema table format.

Raises:
- ValueError: If the provided data is not for the specified target table.
- Exception: If an error occurs during the transformation process.
"""


def transform_sales_order(sales_order_data: str | dict):
    if isinstance(sales_order_data, str):
        table_data = json.loads(sales_order_data)
    else:
        table_data = sales_order_data

    if table_data["table_name"] != "sales_order":
        log.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]

        df = pd.DataFrame(data, columns=column_names)

        df["created_at"] = pd.to_datetime(df["created_at"], format='ISO8601')
        df["last_updated"] = pd.to_datetime(
            df["last_updated"], format='ISO8601')
        df["agreed_payment_date"] = pd.to_datetime(
            df["agreed_payment_date"], format='ISO8601').dt.date
        df["agreed_delivery_date"] = pd.to_datetime(
            df["agreed_delivery_date"], format='ISO8601').dt.date

        df["created_date"] = df["created_at"].dt.date
        df["created_time"] = df["created_at"].dt.time
        df["last_updated_date"] = df["last_updated"].dt.date
        df["last_updated_time"] = df["last_updated"].dt.time

        df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)

        df["sales_record_id"] = range(1, 1 + df.shape[0])
        expected_columns = [
            "sales_record_id",
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]

        fact_sales_order = df[expected_columns]
        return fact_sales_order

    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e


def transform_address(address_data: str | dict):
    if isinstance(address_data, str):
        table_data = json.loads(address_data)
    else:
        table_data = address_data

    if table_data["table_name"] != "address":
        log.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]
        dim_location = pd.DataFrame(data, columns=column_names)

        address_columns_to_keep = [
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

        dim_location = dim_location[address_columns_to_keep]
        dim_location.rename(columns={"address_id": "location_id"},
                            inplace=True)

        return dim_location
    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e


def transform_design(design_data: str | dict):
    if isinstance(design_data, str):
        table_data = json.loads(design_data)
    else:
        table_data = design_data

    if table_data["table_name"] != "design":
        log.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]
        dim_design = pd.DataFrame(data, columns=column_names)

        design_columns_to_keep = [
            "design_id",
            "design_name",
            "file_location",
            "file_name",
        ]

        dim_design = dim_design[design_columns_to_keep]
        return dim_design
    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e


def transform_purchase_order(purchase_order_data: str | dict):
    if isinstance(purchase_order_data, str):
        table_data = json.loads(purchase_order_data)
    else:
        table_data = purchase_order_data
    if table_data["table_name"] != "purchase_order":
        log.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]
        df = pd.DataFrame(data, columns=column_names)
        df["created_at"] = pd.to_datetime(df["created_at"], format='ISO8601')
        df["last_updated"] = pd.to_datetime(
            df["last_updated"], format='ISO8601')
        df["agreed_delivery_date"] = pd.to_datetime(
            df["agreed_delivery_date"], format='ISO8601').dt.date
        df["agreed_payment_date"] = pd.to_datetime(
            df["agreed_payment_date"], format='ISO8601').dt.date

        df["created_date"] = df["created_at"].dt.date
        df["created_time"] = df["created_at"].dt.time
        df["last_updated_date"] = df["last_updated"].dt.date
        df["last_updated_time"] = df["last_updated"].dt.time

        df["purchase_record_id"] = range(1, 1 + df.shape[0])

        expected_columns = [
            "purchase_record_id",
            "purchase_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "staff_id",
            "counterparty_id",
            "item_code",
            "item_quantity",
            "item_unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
        ]

        fact_purchase_order = df[expected_columns]

        return fact_purchase_order
    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e


def transform_payment(payment_data: str | dict):
    if isinstance(payment_data, str):
        table_data = json.loads(payment_data)
    else:
        table_data = payment_data
    if table_data["table_name"] != "payment":
        log.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]
        df = pd.DataFrame(data, columns=column_names)

        df["created_at"] = pd.to_datetime(df["created_at"], format='ISO8601')
        df["last_updated"] = pd.to_datetime(
            df["last_updated"], format='ISO8601')
        df["payment_date"] = pd.to_datetime(
            df["payment_date"], format='ISO8601').dt.date

        df["created_date"] = df["created_at"].dt.date
        df["created_time"] = df["created_at"].dt.time
        df["last_updated_date"] = df["last_updated"].dt.date
        df["last_updated"] = df["last_updated"].dt.time

        df["payment_record_id"] = range(1, 1 + df.shape[0])

        expected_columns = [
            "payment_record_id",
            "payment_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated",
            "transaction_id",
            "counterparty_id",
            "payment_amount",
            "currency_id",
            "payment_type_id",
            "paid",
            "payment_date",
        ]

        fact_payment = df[expected_columns]

        return fact_payment
    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e


def transform_transaction(transaction_data: str | dict):
    if isinstance(transaction_data, str):
        table_data = json.loads(transaction_data)
    else:
        table_data = transaction_data

    if table_data["table_name"] != "transaction":
        log.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]
        dim_transaction = pd.DataFrame(data, columns=column_names)

        columns_to_keep = [
            "transaction_id",
            "transaction_type",
            "sales_order_id",
            "purchase_order_id"
        ]

        dim_transaction = dim_transaction[columns_to_keep]
        dim_transaction.fillna(0, inplace=True)
        dim_transaction["purchase_order_id"] = dim_transaction[
            'purchase_order_id'].astype(int)
        dim_transaction["sales_order_id"] = dim_transaction[
            'sales_order_id'].astype(int)
        return dim_transaction
    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e


def transform_payment_type(payment_type_data):
    if isinstance(payment_type_data, str):
        table_data = json.loads(payment_type_data)
    else:
        table_data = payment_type_data

    if table_data["table_name"] != "payment_type":
        log.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]
        dim_payment_type = pd.DataFrame(data, columns=column_names)

        columns_to_keep = [
            "payment_type_id",
            "payment_type_name"
        ]

        dim_payment_type = dim_payment_type[columns_to_keep]
        return dim_payment_type
    except Exception as e:
        log.error(f"Error: {str(e)}")
        raise e
