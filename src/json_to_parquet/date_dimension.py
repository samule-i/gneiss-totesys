from datetime import datetime as dt
from datetime import timedelta
from utils.custom_log import totesys_logger
import math
import pandas as pd

log = totesys_logger()


def date_dimension(start_date: str = '2022-01-01',
                   duration_in_years: int = 5) -> pd.DataFrame:
    ''' start_date='2022-01-01', duration_in_years=5

    Returns a pandas dataframe with every day in the range given.
    '''
    dimension = {
        'date_id': [],
        'year': [],
        'month': [],
        'day': [],
        'day_of_week': [],
        'day_name': [],
        'month_name': [],
        'quarter': []
    }

    date = dt.fromisoformat(start_date)

    days = 365 * duration_in_years
    for i in range(days):
        month_number = date.strftime('%-m')
        quarter = math.ceil(int(month_number) / 3)
        dimension['date_id'].append((date.strftime('%Y-%m-%d')))
        dimension['year'].append(int(date.strftime('%Y')))
        dimension['month'].append(int(date.strftime('%m')))
        dimension['day'].append(int(date.strftime('%-d')))
        dimension['day_of_week'].append(int(date.strftime('%w')))
        dimension['day_name'].append(date.strftime('%A'))
        dimension['month_name'].append(date.strftime('%B'))
        dimension['quarter'].append(quarter)

        date += timedelta(days=1)
    df = pd.DataFrame(dimension)
    df["date_id"] = pd.to_datetime(
        df["date_id"]).dt.date

    return df
