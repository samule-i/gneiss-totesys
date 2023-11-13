from json_to_parquet.date_dimension import date_dimension


def test_increments_id():
    result = date_dimension('2023-11-08', 1)
    print(result)
    result['date_id'] = result[
        'date_id'].astype(str)
    assert result.loc[0, "date_id"] == '2023-11-08'
    assert result.loc[1, "date_id"] == '2023-11-09'
    assert result.loc[4, "date_id"] == '2023-11-12'


def test_gets_year_correct():
    result = date_dimension('2023-11-08', 1)
    assert result.loc[0, "year"] == 2023
    assert result.loc[360, "year"] == 2024


def test_gets_month_correct():
    result = date_dimension('2023-11-08', 1)
    assert result.loc[0, "month"] == 11
    assert result.loc[30, "month"] == 12


def test_gets_day_correct():
    result = date_dimension('2023-11-08', 1)
    assert result.loc[0, "day"] == 8
    assert result.loc[1, "day"] == 9


def test_gets_day_of_week_correct():
    result = date_dimension('2023-11-08', 1)
    assert result.loc[0, "day_of_week"] == 3
    assert result.loc[1, "day_of_week"] == 4


def test_gets_day_name_correct():
    result = date_dimension('2023-11-08', 1)
    assert result.loc[0, "day_name"] == 'Wednesday'
    assert result.loc[1, "day_name"] == 'Thursday'


def test_gets_month_name_correct():
    result = date_dimension('2023-11-08', 1)
    assert result.loc[0, "month_name"] == 'November'
    assert result.loc[30, "month_name"] == 'December'


def test_gets_quarter_correct():
    result = date_dimension('2023-01-01', 1)
    assert result.loc[0, "quarter"] == 1

    result = date_dimension('2023-04-01', 1)
    assert result.loc[0, "quarter"] == 2

    result = date_dimension('2023-07-01', 1)
    assert result.loc[0, "quarter"] == 3

    result = date_dimension('2023-10-01', 1)
    assert result.loc[0, "quarter"] == 4


def test_includes_multiple_rows():
    result = date_dimension('2023-11-08', 1)
    assert len(result) == 365
