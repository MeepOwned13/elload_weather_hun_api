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


class DatabaseStructureTests(unittest.TestCase, DatabaseConnect):
    # Tests for the existence of tables, views

    def setUp(self):
        # Set up new connection before test
        DatabaseConnect.__init__(self, db_connect_info, null_logger)

    def tearDown(self):
        # Close old connection after test
        DatabaseConnect.__del__(self)

    def __del__(self):
        # DatabaseConnect.__del__ is already called after every test
        pass

    @DatabaseConnect._db_transaction
    def test_omsz_tables_views(self):
        # Test existence of OMSZ tables, views
        self._curs.execute("SHOW FULL TABLES")
        data = [entry[0].lower() for entry in self._curs.fetchall()]
        self.assertIn("omsz_meta", data)
        self.assertIn("omsz_status", data)
        self.assertIn("omsz_data", data)

    @DatabaseConnect._db_transaction
    def test_mavir_tables_views(self):
        # Test existence of OMSZ tables, views
        self._curs.execute("SHOW FULL TABLES")
        data = [entry[0].lower() for entry in self._curs.fetchall()]
        self.assertIn("mavir_status", data)
        self.assertIn("mavir_data", data)

    @DatabaseConnect._db_transaction
    def test_ai_tables_views(self):
        # Test existence of OMSZ tables, views
        self._curs.execute("SHOW FULL TABLES")
        data = [entry[0].lower() for entry in self._curs.fetchall()]
        self.assertIn("ai_10min", data)
        self.assertIn("ai_1hour", data)

    @DatabaseConnect._db_transaction
    def test_s2s_tables_views(self):
        # Test existence of OMSZ tables, views
        self._curs.execute("SHOW FULL TABLES")
        data = [entry[0].lower() for entry in self._curs.fetchall()]
        self.assertIn("s2s_status", data)
        self.assertIn("s2s_raw_preds", data)
        self.assertIn("s2s_aligned_preds", data)

