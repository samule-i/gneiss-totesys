from datetime import datetime as dt
from datetime import timedelta
import math
import pandas as pd


def date_dimension(start_date: str = '2022-01-01', duration_in_years: int = 5):
    ''' start_date='2022-01-01', duration_in_years=5

    Returns a pandas dataframe with every day in the range given.
    '''
    dimension = {
        'year': [],
        'month': [],
        'day': [],
        'day_of_week': [],
        'day_name': [],
        'month_name': [],
        'quarter': []
    }
    day_to_weekday_map = {
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6,
        'Sunday': 7
    }
    date = dt.fromisoformat(start_date)

    days = 365*duration_in_years
    for _ in range(days):
        month_number = date.strftime('%-m')
        quarter = math.ceil(int(month_number)/3)
        # quarter = math.ceil(int(month_number)/4)

        dimension['year'].append(int(date.strftime('%Y')))
        dimension['month'].append(int(date.strftime('%m')))
        dimension['day'].append(int(date.strftime('%-d')))
        dimension['day_of_week'].append(int(date.strftime('%w')))
        dimension['day_name'].append(date.strftime('%A'))
        dimension['month_name'].append(date.strftime('%B'))
        dimension['quarter'].append(quarter)

        date += timedelta(days=1)
    df = pd.DataFrame(dimension)

    return df
