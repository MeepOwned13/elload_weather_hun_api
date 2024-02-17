from requests import Session
from pathlib import Path
import logging
import sqlite3
import pandas as pd
import io
import re
import warnings
from datetime import datetime

logger = logging.getLogger("mavir")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())


class MAVIR_Downloader():
    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._con: sqlite3.Connection = sqlite3.connect(self._db_path, timeout=120, autocommit=False)
        self._curs: sqlite3.Cursor = None
        self._sess: Session = Session()

    def __del__(self):
        if self._con:
            self._con.close()

    def _db_transaction(func):
        """
        This function opens a cursor at self._curs and makes sure the decorated function is a single transaction.
        Exceptions to this rule are pd.df.to_sql() table creations, so they should only be used for temporary tables.
        """

        def execute(self, *args, **kwargs):
            with self._con as self._curs:
                logger.debug("Database transaction begin")
                res = func(self, *args, **kwargs)
                self._curs.commit()
            logger.debug("Database transaction commit")
            return res
        return execute

    def _download_data(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None:
        """
        Download data based on timeframe
        :param start: Start time in UTC
        :param end: End time in UTC
        :return: Downloaded DataFrame
        """
        logger.debug(f"Requesting electricity data from {start} to {end}")
        url = (f"https://www.mavir.hu/rtdwweb/webuser/chart/7678/export"
               f"?exportType=xlsx"
               f"&fromTime={int(start.value / 1e6)}"
               f"&toTime={int(end.value / 1e6)}"
               f"&periodType=min"
               f"&period=10")

        request = self._sess.get(url)
        if request.status_code != 200:
            logger.error(f"Electricity data download failed from {start} to {end}")
            return
        logger.debug(f"Recieved electricity data from {start} to {end}")

        xlsx = io.BytesIO(request.content)

        # Ignore warning related to openpyxl using default style because the Excel doesn't contain any
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module=re.escape('openpyxl.styles.stylesheet'))
            df = pd.read_excel(xlsx, skiprows=0, parse_dates=True, engine='openpyxl')

        return df

