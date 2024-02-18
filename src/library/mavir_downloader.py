from requests import Session
from pathlib import Path
import logging
import sqlite3
import pandas as pd
import io
import re
import warnings
from datetime import datetime

mavir_downloader_logger = logging.getLogger("mavir")
mavir_downloader_logger.setLevel(logging.DEBUG)
mavir_downloader_logger.addHandler(logging.NullHandler())


class MAVIR_Downloader():
    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._con: sqlite3.Connection = sqlite3.connect(self._db_path, timeout=120, autocommit=False)
        self._curs: sqlite3.Cursor = None
        self._sess: Session = Session()
        self._RENAME: dict = {"Időpont": "Time",  # Time of data
                              # Net load and estimates
                              "Nettó terhelés": "NetSystemLoad",
                              "Nettó rendszerterhelés tény - üzemirányítási": "NetSystemLoadFactPlantManagment",
                              "Nettó tény rendszerterhelés - net.ker.elsz.meres": "NetSystemLoadNetTradeSettlement",
                              "Nettó terv rendszerterhelés": "NetPlanSystemLoad",
                              "Nettó rendszerterhelés becslés (dayahead)": "NetSystemLoadDayAheadEstimate",
                              "Nettó terv rendszertermelés": "NetPlanSystemProduction",
                              # Gross load and estimates
                              "Bruttó tény rendszerterhelés": "GrossSystemLoad",
                              "Bruttó hitelesített rendszerterhelés tény": "GrossCertifiedSystemLoad",
                              "Bruttó terv rendszerterhelés": "GrossPlanSystemLoad",
                              "Bruttó rendszerterhelés becslés (dayahead)": "GrossSystemLoadDayAheadEstimate",
                              }

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
                mavir_downloader_logger.debug("Database transaction begin")
                res = func(self, *args, **kwargs)
                self._curs.commit()
            mavir_downloader_logger.debug("Database transaction commit")
            return res
        return execute

    def _format_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.rename(columns=self._RENAME, inplace=True)
        df = df[self._RENAME.values()]  # Reordering, assumes all columns exist
        # Not dropping empty columns since they will be filled later when data is available
        # Using apply here because day/night saving transition doesn't translate well to datetime types
        df["Time"] = df["Time"].apply(
            lambda o: pd.to_datetime(o, format="%Y.%m.%d %H:%M:%S %z").tz_convert("UTC").tz_localize(None))
        df.index = df['Time']  # Time is stored in UTC
        df.drop('Time', axis=1, inplace=True)  # index creates duplicate
        # Dropping last row, since it always contained NaN values
        df.drop(df.tail(1).index, inplace=True)
        return df

    def _download_data(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None:
        """
        Download data based on timeframe
        Start and end time should result in at most 60_000 entries if period is 10 minutes
        WARNING: don't call this function in a loop, call _download_data_range instead
        :param start: Start time in UTC, non-inclusive
        :param end: End time in UTC, inclusive
        :return: Downloaded DataFrame
        """
        mavir_downloader_logger.debug(f"Requesting electricity data from {start} to {end}")
        url = (f"https://www.mavir.hu/rtdwweb/webuser/chart/7678/export"
               f"?exportType=xlsx"
               f"&fromTime={int(start.value / 1e6)}"
               f"&toTime={int(end.value / 1e6)}"
               f"&periodType=min"
               f"&period=10")

        request = self._sess.get(url)
        if request.status_code != 200:
            mavir_downloader_logger.error(
                f"Electricity data download failed from {start} to {end} with {request.status_code}")
            return
        mavir_downloader_logger.info(f"Recieved electricity data from {start} to {end}")

        xlsx = io.BytesIO(request.content)

        # Ignore warning related to openpyxl using default style because the Excel doesn't contain any
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module=re.escape('openpyxl.styles.stylesheet'))
            df = pd.read_excel(xlsx, skiprows=0, engine='openpyxl')

        return self._format_data(df)

    def _download_data_range(self, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None:
        """
        Download data based on range, allows for any number of entries
        WARNING: don't call this function in a loop, MAVIR API limits request amounts per minute
        :param start: Start time in UTC, inclusive
        :param end: End time in UTC, inclusive
        :return: Downloaded DataFrame
        """
        ls_df = []
        # Removing 10 minutes to get inlcusive start
        start = start - pd.Timedelta(minutes=10)
        # Get all the data in the time range by requests of range 600_000 minutes at once
        while start < end:
            new_start = start + pd.Timedelta(minutes=10 * 59_999)
            if new_start >= end:
                new_start = end
            ls_df.append(self._download_data(start, new_start))
            start = new_start

        return pd.concat(ls_df)

