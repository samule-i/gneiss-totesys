import pandas as pd
import os
import json
from json_to_parquet.get_s3_file import json_from_row_id
from utils.custom_log import totesys_logger
log = totesys_logger()


def dim_staff(data: dict | str) -> pd.DataFrame:
    bucket = os.environ['S3_DATA_ID']
    foreign_columns = ['department_id',
                       'location',
                       'created_at',
                       'last_updated']
    foreign_table: pd.DataFrame = pd.DataFrame(columns=foreign_columns)
    foreign_table_name = 'department'
    foreign_key = 'department_id'
    discard_columns = ['manager', 'department_id']
    valid_tables = ['staff']

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

    foreign_ids = dim[foreign_key].unique()

    for id in foreign_ids:
        data = json_from_row_id(bucket, foreign_table_name, id)
        _df = pd.DataFrame(data['data'], columns=data['column_names'])
        foreign_table = pd.concat([foreign_table, _df])
        foreign_table.drop(
            columns=['created_at', 'last_updated'], inplace=True)
    foreign_table.drop_duplicates(inplace=True)

    dim = dim.merge(foreign_table, on=foreign_key)
    dim.drop(columns=discard_columns, inplace=True)
    return dim
