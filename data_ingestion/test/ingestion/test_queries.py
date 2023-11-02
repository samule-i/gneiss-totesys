from ingestion.queries import get_table_extract_sql
import pytest


def test_get_table_extract_sql_returns_correct_result_with_timestamp():
    query_str = get_table_extract_sql("sales_order")
    assert query_str == (
        "SELECT * FROM sales_order "
        "WHERE last_updated > :previous_timestamp;"
    )


def test_get_table_extract_sql_raises_error_for_invalid_table_name():
    with pytest.raises(ValueError):
        get_table_extract_sql("not_a_table")
