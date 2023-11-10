import pandas as pd
import json
import logging


def transform_sales_order(sales_order_data):
    table_data = json.loads(sales_order_data)
    if table_data["table_name"] != "sales_order":
        logging.error("Invalid Table Name.")
        raise ValueError("Invalid Table Name.")
    try:
        column_names = table_data["column_names"]
        data = table_data["data"]
        df = pd.DataFrame(data, columns=column_names)
        df.insert(0, "sales_record_id", range(1, 1 + df.shape[0]))
        df["created_date"] = df["created_at"].str.split(" ").str[0]
        df["created_time"] = df["created_at"].str.split(" ").str[1]
        df["last_updated_date"] = df["last_updated"].str.split(" ").str[0]
        df["last_updated_time"] = df["last_updated"].str.split(" ").str[1]

        df.rename(columns={"staff_id": "sales_staff_id"},
                  inplace=True)
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
        logging.error(f"Error: {str(e)}")
        raise e


def transform_address(address_data):
    table_data = json.loads(address_data)
    if table_data["table_name"] != "address":
        logging.error("Invalid Table Name.")
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
        logging.error(f"Error: {str(e)}")
        raise e


def transform_design(design_data):
    table_data = json.loads(design_data)
    if table_data["table_name"] != "design":
        logging.error("Invalid Table Name.")
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
        logging.error(f"Error: {str(e)}")
        raise e
