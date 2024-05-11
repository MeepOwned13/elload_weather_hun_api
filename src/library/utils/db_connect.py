import logging
import pandas as pd
import mysql.connector as connector
import numpy as np


class DatabaseConnect:
    """
    Inherit this class to have Database Connection with custom transaction handling
    It also requires a logger, which can be used the child class
    CALLING __del__ IN CHILD CLASSES IS REQUIRED FOR CLOSING THE CONNECTION
    """

    def __init__(self, db_connect_info: dict, logger: logging.Logger):
        """
        Tests database connection on init
        :param db_connect_info: should contain “host”, “user”, “password”, “database”
        :param logger: logger to use
        """
        self._con: connector.CMySQLConnection = connector.connect(**db_connect_info)
        self._curs: connector.cursor_cext.CMySQLCursor = None
        self._logger: logging.Logger = logger
        self._in_transaction = False

    def __del__(self):
        if self._curs:
            self._curs.close()
        if self._con:
            self._con.close()

    @staticmethod
    def _db_transaction(func):
        """
        This function opens a cursor at self._curs and makes sure the decorated function is a single transaction.
        """

        def execute(self, *args, **kwargs):
            if not self._con.is_connected():
                self._con.reconnect(attempts=2, delay=5)
            try:
                # Start transcation
                self._in_transaction = True
                self._curs = self._con.cursor()
                self._curs.execute("BEGIN")
                self._logger.debug("Database transaction begin")
                # Execute decorated function
                res = func(self, *args, **kwargs)
                # Finish, commit transaction
                self._curs.execute("COMMIT")
                self._logger.debug("Database transaction commit")
            except Exception:
                if self._curs:
                    # If a cursor exits, roll back everything
                    self._curs.execute("ROLLBACK")
                    self._logger.debug("Database transaction rollback")
                raise
            finally:
                if self._curs:
                    self._curs.close()
                self._in_transaction = False
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

    def _df_cols_to_sql_cols(self, df: pd.DataFrame):
        """
        Convert column names to SQL viable string, needs at least 1 column
        Useful to specify columns of Tables at insertion to avoid problems with orders
        :param df: DataFrame to get columns of
        :returns: SQL compatible string of column names
        """
        cols = tuple(df.columns)
        if df.index.name:
            cols = (df.index.name,) + cols

        if len(cols) < 1:
            raise ValueError("No columns")

        # " instead of ', also removing () after tuple->str
        col_str = str(cols).replace("'", "")[1:-1]
        if len(cols) == 1:
            col_str = col_str[:-1]  # tuple->str leaves a ',' if it has a single element

        return col_str

    @_assert_transaction
    def _df_to_sql(self, df: pd.DataFrame, table: str, method: str = 'INSERT IGNORE', unpack_index: bool = True):
        """
        Expects that df is indexed via datetime or timestamp
        Df is modified in this process
        :param df: DataFrame to insert
        :param table: table name to insert into
        :param method: INSERT, INSERT IGNORE or REPLACE
        :param unpack_index: True if data in Index should be inserted into table
        """
        if method not in ('INSERT', 'INSERT IGNORE', 'REPLACE'):
            raise ValueError("method must be INSERT or REPLACE")

        # executemany knows None, but won't recognize the others
        df.replace({np.nan: None, pd.NaT: None}, inplace=True)
        if unpack_index:
            df.reset_index(inplace=True)

        cols = self._df_cols_to_sql_cols(df)
        # Executemany uses %s marks for placeholders
        marks = "%s," * len(df.columns)
        vals = df.values
        # Batched insert
        for i in range(0, len(df), 4096):
            inserts = [(*elements,) for elements in vals[i:i + 4096]]
            self._curs.executemany(
                f"{method} INTO {table} ({cols}) VALUES ({marks[:-1]})", inserts)
