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


def transform_staff(staff_json, department_json):
    staff_dict = json.loads(staff_json)
    department_dict = json.loads(department_json)
    if (
        staff_dict["table_name"] != "staff"
        or department_dict["table_name"] != "department"
    ):
        logging.error("Invalid Table Name.")
        raise ValueError()
    try:
        staff_dict = json.loads(staff_json)
        staff_data = staff_dict["data"]
        staff_column_names = staff_dict["column_names"]
        staff_df = pd.DataFrame(staff_data, columns=staff_column_names)

        department_dict = json.loads(department_json)
        department_data = department_dict["data"]
        department_column_names = department_dict["column_names"]
        department_df = pd.DataFrame(department_data,
                                     columns=department_column_names)

        result = pd.merge(
            staff_df, department_df, how="inner",
            on=["department_id", "department_id"]
        )

        columns_to_keep = [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]

        dim_staff = result[columns_to_keep]

        return dim_staff
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise e


def transform_counterparty(counterparty_json, address_json):
    counterparty_dict = json.loads(counterparty_json)
    address_dict = json.loads(address_json)
    if (
        counterparty_dict["table_name"] != "counterparty"
        or address_dict["table_name"] != "address"
    ):
        logging.error("Invalid Table Name.")
        raise ValueError()
    try:
        counterparty_column_names = counterparty_dict["column_names"]
        counterparty_data = counterparty_dict["data"]
        counterparty_df = pd.DataFrame(
            counterparty_data, columns=counterparty_column_names
        )
        address_column_names = address_dict["column_names"]
        address_data = address_dict["data"]
        address_df = pd.DataFrame(address_data, columns=address_column_names)

        address_df.rename(columns={"address_id": "legal_address_id"},
                          inplace=True)

        result = pd.merge(
            counterparty_df,
            address_df,
            how="inner",
            on=["legal_address_id", "legal_address_id"],
        )
        result.rename(
            columns={
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone": "counterparty_legal_phone_number",
            },
            inplace=True,
        )

        columns_to_keep = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]

        dim_counterparty = result[columns_to_keep]

        return dim_counterparty
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise e