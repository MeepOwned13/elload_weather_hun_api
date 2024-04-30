import unittest
import logging
from dotenv import dotenv_values
from src.library.utils.db_connect import DatabaseConnect

null_logger = logging.getLogger("foo")
null_logger.addHandler(logging.NullHandler())

db_connect_info = dotenv_values(".env")
db_connect_info = {
    "host": db_connect_info["HOST"],
    "user": db_connect_info["USER"],
    "password": db_connect_info["PASW"],
    "database": db_connect_info["DBNM"]
}


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
        self.assertTrue(self._con.is_connected())

    @DatabaseConnect._db_transaction
    def test_transaction(self):
        self.assertIsNotNone(self._curs)

    def test_invalid_assert_transaction(self):
        self.assertRaises(RuntimeError, self.assert_transaction)

    @DatabaseConnect._db_transaction
    def test_valid_assert_transaction(self):
        try:
            self.assert_transaction()
        except RuntimeError:
            self.fail("self.assert_transaction raised RuntimeError unexpectedly")

