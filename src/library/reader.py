import logging
from pathlib import Path
import sqlite3
import pandas as pd


reader_logger = logging.getLogger("omsz")
reader_logger.setLevel(logging.DEBUG)
reader_logger.addHandler(logging.NullHandler())


class Reader():
    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._con: sqlite3.Connection = sqlite3.connect(
            self._db_path, timeout=120, autocommit=False, check_same_thread=False)
        self._curs: sqlite3.Cursor = None

    def __del__(self):
        if self._con:
            self._con.close()

    def _db_transaction(func):
        """
        This function opens a cursor at self._curs and makes sure the decorated function is a single transaction.
        """

        def execute(self, *args, **kwargs):
            with self._con as self._curs:
                reader_logger.debug("Database transaction begin")
                res = func(self, *args, **kwargs)
                self._curs.commit()
            reader_logger.debug("Database transaction commit")
            return res
        return execute

    @_db_transaction
    def get_weather_meta(self) -> pd.DataFrame:
        reader_logger.info("Reading OMSZ_meta")
        df = pd.read_sql("SELECT * FROM OMSZ_meta", con=self._con)
        return df

