import sqlite3
from pathlib import Path
import logging


class DatabaseConnect():
    """
    Inherit this class to have Database Connection with custom transaction handling
    It also requires a logger, which can be used the child class
    """

    def __init__(self, db_path: Path, logger: logging.Logger):
        self._db_path: Path = db_path
        self._con: sqlite3.Connection = sqlite3.connect(
            self._db_path, timeout=120, autocommit=False, check_same_thread=False)
        self._curs_: sqlite3.Cursor = None  # _ after a variable means it's not initiated druing __init__, but later
        self._logger: logging.Logger = logger
        self._in_transaction = False

    def __del__(self):
        if self._con:
            self._con.close()

    @staticmethod
    def _db_transaction(func):
        """
        This function opens a cursor at self._curs and makes sure the decorated function is a single transaction.
        """

        def execute(self, *args, **kwargs):
            try:
                self._in_transaction = True
                with self._con as self._curs_:
                    self._logger.debug("Database transaction begin")
                    res = func(self, *args, **kwargs)
                    self._curs_.commit()
            finally:
                self._in_transaction = False
            self._logger.debug("Database transaction commit")
            return res
        return execute

    @staticmethod
    def _assert_transaction(func):
        """
        Checks if there is an ongoing transaction before executing the function.
        """

        def execute(self, *args, **kwargs):
            if not self._in_transaction:
                raise RuntimeError("Function requires an ongoing transaction")
            return func(self, *args, **kwargs)
        return execute

