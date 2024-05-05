import unittest
import logging
from dotenv import dotenv_values
from src.library.utils.db_connect import DatabaseConnect
import pandas as pd
import numpy as np

null_logger = logging.getLogger("foo")
null_logger.addHandler(logging.NullHandler())

db_connect_info = dotenv_values(".env")
db_connect_info = {
    "host": db_connect_info["HOST"],
    "user": db_connect_info["USER"],
    "password": db_connect_info["PASW"],
    "database": db_connect_info["DBNM"]
}

# MD4 hash generated from "elload_hun_weather_api" text on https://www.browserling.com/tools/all-hashes
# + "_test_table" text
test_table_name = "941618813b794331aa5c5dbb1e097c38_test_table"


class DatabaseConnectTests(unittest.TestCase, DatabaseConnect):
    # Test DatabaseConnect class

    def setUp(self):
        # Set up new connection before test
        DatabaseConnect.__init__(self, db_connect_info, null_logger)

    def tearDown(self):
        # Close old connection after test
        DatabaseConnect.__del__(self)

    def __del__(self):
        # DatabaseConnect.__del__ is already called after every test
        pass

    @DatabaseConnect._assert_transaction
    def assert_transaction(self):
        # Utility function used in testing DatabaseConnect._assert_transaction
        self.assertTrue(True)

    def test_connect(self):
        # Test connection
        self.assertTrue(self._con.is_connected())

    @DatabaseConnect._db_transaction
    def test_transaction(self):
        # Test cursor creation
        self.assertIsNotNone(self._curs)

    def test_invalid_assert_transaction(self):
        # Test invalid use of @_assert_transaction
        self.assertRaises(RuntimeError, self.assert_transaction)

    @DatabaseConnect._db_transaction
    def test_valid_assert_transaction(self):
        # Test valid use of @_assert_transaction
        try:
            self.assert_transaction()
        except RuntimeError:
            self.fail("self.assert_transaction raised RuntimeError unexpectedly")

    @DatabaseConnect._db_transaction
    def create_test_table(self):
        self._curs.execute(
            f"""
            CREATE TABLE {test_table_name}(
                Id INT PRIMARY KEY,
                Data REAL,
                Text TEXT
                )""")

    @DatabaseConnect._db_transaction
    def delete_test_table(self):
        self._curs.execute(f"DROP TABLE IF EXISTS {test_table_name}")

    def create_delete_test_table(func):
        def execute(self, *args, **kwargs):
            res = None
            try:
                self.create_test_table()
                res = func(self, *args, **kwargs)
            finally:
                self.delete_test_table()
            return res
        return execute

    @create_delete_test_table
    def test_table_creation(self):
        # Test creation of table
        @DatabaseConnect._db_transaction
        def exists_test_table(self):
            self._curs.execute(f"SHOW TABLES LIKE '{test_table_name}'")
            self.assertGreater(len(self._curs.fetchall()), 0)

        exists_test_table(self)

    @create_delete_test_table
    def test_df_to_sql(self):
        # Test insertion from pandas.DataFrame
        @DatabaseConnect._db_transaction
        def insert(self):
            df = pd.DataFrame(data={"Id": [1, 2], "Data": [0.5, np.nan], "Text": ["Hi", "Bye"]})
            self._df_to_sql(df, test_table_name, unpack_index=False)

        @DatabaseConnect._db_transaction
        def check(self):
            self._curs.execute(f"SELECT Id, Data, Text FROM {test_table_name}")
            data = self._curs.fetchall()
            # First row
            self.assertEqual(data[0][0], 1)
            self.assertAlmostEqual(data[0][1], 0.5, 0.01)
            self.assertEqual(data[0][2], "Hi")
            # Second row
            self.assertEqual(data[1][0], 2)
            self.assertIsNone(data[1][1])
            self.assertEqual(data[1][2], "Bye")

        insert(self)
        check(self)

    @create_delete_test_table
    def test_rollback(self):
        # Test rollback of @_db_transaction
        @DatabaseConnect._db_transaction
        def insert_with_error(self):
            df = pd.DataFrame(data={"Id": [1, 2], "Data": [0.5, np.nan], "Text": ["Hi", "Bye"]})
            self._df_to_sql(df, test_table_name, unpack_index=False)
            raise RuntimeError("Intentional error during insert")

        @DatabaseConnect._db_transaction
        def check(self):
            self._curs.execute(f"SELECT Id, Data, Text FROM {test_table_name}")
            data = self._curs.fetchall()
            self.assertEqual(len(data), 0)

        try:
            insert_with_error(self)
        except RuntimeError:
            check(self)

