class IncompatibleTableException(Exception):
    pass


def currency_transform(stored_data):
    if stored_data["table_name"] != 'currency':
        raise IncompatibleTableException
    currency_name_mapping = {
        'GBP': 'Great British Pound',
        'USD': 'US Dollar',
        'EUR': 'Euro'
    }
    rows = stored_data['data']
    currency = {'currency_id':   [field[0] for field in rows],
                'currency_code': [field[1] for field in rows],
                'currency_name': [
                    currency_name_mapping[field[1]] for field in rows]
                }
    return currency
