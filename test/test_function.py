from src.function import convert_psql_table_to_json
import unittest
import os


class TestConvertPsqlTableToJson(unittest.TestCase):
    def test_convert_table_to_json(self):
        host = 'localhost'
        database = 'test_database'
        user = os.getlogin()
        password = os.environ.get("DB_PASSWORD")
        table_name = 'test_table'
        json_result = convert_psql_table_to_json(
            host, database, user, password, table_name)

        self.assertTrue(isinstance(json_result, str))
        self.assertTrue(json_result.startswith("["))
        self.assertTrue(json_result.endswith("]"))
        self.assertIn("column1", json_result)
        self.assertIn("column2", json_result)
        self.assertIn("column3", json_result)
        self.assertNotEqual(json_result, "Error: ")

    def test_empty_table_to_json(self):
        host = 'localhost'
        database = 'test_database'
        user = os.getlogin()
        password = os.environ.get("DB_PASSWORD")
        table_name = 'empty_table'
        json_result = convert_psql_table_to_json(
            host, database, user, password, table_name)
        self.assertEqual(json_result, "[]")

    def test_nonexistent_table(self):
        host = 'localhost'
        database = 'empty_test_database'
        user = os.getlogin()
        password = os.environ.get("DB_PASSWORD")
        table_name = "nonexistent_table"
        json_result = convert_psql_table_to_json(
            host, database, user, password, table_name)
        self.assertTrue(json_result.startswith("Error: "))
