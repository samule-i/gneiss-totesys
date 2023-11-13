import pandas as pd
import json


def currency_transform(data: dict | str) -> pd.DataFrame:
    if isinstance(data, str):
        table = json.loads(data)
    else:
        table = data
    if table["table_name"] != 'currency':
        raise ValueError('Invalid table_name')
    currency_name_mapping = {
        'GBP': 'Great British Pound',
        'USD': 'US Dollar',
        'EUR': 'Euro'
    }
    rows = table['data']
    try:
        currency = {'currency_id':   [field[0] for field in rows],
                    'currency_code': [field[1] for field in rows],
                    'currency_name': [
                        currency_name_mapping[field[1]] for field in rows]
                    }
    except KeyError as e:
        raise e
    df = pd.DataFrame(currency)
    return df
