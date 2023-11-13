import pandas as pd
import os
import json
from json_to_parquet.get_s3_file import json_from_row_id
from utils.custom_log import logger
log = logger(__name__)


def dim_counterparty(data: dict | str) -> pd.DataFrame:
    bucket = os.environ['S3_DATA_ID']
    foreign_columns = ['address_id',
                       'address_line_1',
                       'address_line_2',
                       'district',
                       'city',
                       'postal_code',
                       'country',
                       'phone',
                       'created_at',
                       'last_updated']
    foreign_table: pd.DataFrame = pd.DataFrame(columns=foreign_columns)
    foreign_table_name = 'address'
    foreign_key = 'address_id'
    foreign_key_map = {'legal_address_id': 'address_id'}
    discard_columns = ['commercial_contact', 'delivery_contact', 'address_id']
    valid_tables = ['staff', 'counterparty']
    dim_column_rename_map = {
        'counterparty_id': 'counterparty_id',
        'counterparty_legal_name': 'counterparty_legal_name',
        'address_line_1': 'counterparty_legal_address_line_1',
        'address_line_2': 'counterparty_legal_address_line_2',
        'district': 'counterparty_legal_district',
        'city': 'counterparty_legal_city',
        'postal_code': 'counterparty_legal_postal_code',
        'country': 'counterparty_legal_country',
        'phone': 'counterparty_legal_phone_number'
    }

    if isinstance(data, str):  # todo: test-suite string vs dict vs other
        table = json.loads(data)
    else:
        table = data

    log.info(
        f'Running with {table["table_name"]}: {table.get("record_count")}')

    if table["table_name"] not in valid_tables:
        log.error(f'{table["table_name"]} is invalid')
        raise ValueError('Invalid table_name')

    dim = pd.DataFrame(table['data'], columns=table['column_names'])
    dim.drop(columns=['created_at', 'last_updated'], inplace=True)
    dim.rename(columns=foreign_key_map, inplace=True)

    foreign_ids = dim[foreign_key].unique()
    print('0')
    print(foreign_ids)
    for id in foreign_ids:
        print('in for')
        print(bucket, foreign_table_name, id)
        data = json_from_row_id(bucket, foreign_table_name, id)
        print('one didnt get here')
        foreign_df = pd.DataFrame(data['data'], columns=data['column_names'])
        print('one')
        foreign_table = pd.concat([foreign_table, foreign_df])
        print('one')
        foreign_table.drop(columns=['created_at', 'last_updated'],
                           inplace=True)
    print('10')
    foreign_table.drop_duplicates(inplace=True)

    print('20')
    dim = dim.merge(foreign_table, on=foreign_key)
    dim.drop(columns=discard_columns, inplace=True)
    dim.rename(columns=dim_column_rename_map, inplace=True)
    return dim
