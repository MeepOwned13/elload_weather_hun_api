from requests import get as req_get
from pathlib import Path
import logging
import sqlite3
from contextlib import closing
import pandas as pd
import io
from zipfile import ZipFile

omsz_logger = logging.getLogger("omsz")
omsz_logger.setLevel(logging.DEBUG)
omsz_logger.addHandler(logging.NullHandler())


class OMSZ_Downloader():
    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._con: sqlite3.Connection = None
        self._curs: sqlite3.Cursor = None

    def _db_connection(func):
        def execute(self,
                    *args, **kwargs):
            with closing(sqlite3.connect(self._db_path)) as self._con, self._con as self._curs:
                omsz_logger.debug("Database connection opened")
                res = func(self, *args, **kwargs)
            omsz_logger.debug("Database connection closed")
            return res
        return execute

    @_db_connection
    def _write_meta(self, df: pd.DataFrame) -> None:
        """
        Write metadata to Database
        :param df: DataFrame to write to Database
        """
        df.to_sql(name="omsz_meta", con=self._con, if_exists="replace")

    def _format_meta(self, meta: pd.DataFrame) -> pd.DataFrame:
        """
        Formats metadata
        :param meta: DataFrame containing metadata
        :return: Formatted metadata DataFrame
        """
        meta.columns = meta.columns.str.strip()  # remove trailing whitespace
        meta.index = meta["StationNumber"]
        meta.drop("StationNumber", axis=1, inplace=True)  # index definition creates duplicate
        meta.dropna(how="all", axis=1, inplace=True)
        meta = meta[~meta.index.duplicated(keep="last")]  # duplicates
        return meta

    def update_meta(self) -> None:
        """
        Downloads metadata and writes it to sqlite Database
        """
        # Request metadata
        url = "https://odp.met.hu/climate/observations_hungary/hourly/station_meta_auto.csv"
        omsz_logger.info(f"Requesting metadata at '{url}'")
        request = req_get(url)
        if request.status_code != 200:
            omsz_logger.error(f"Meta data download failed with {request.status_code} | {url}")
            return
        omsz_logger.debug(f"Meta data recieved from '{url}'")

        # Load data, format and write to DB
        df: pd.DataFrame = pd.read_csv(io.StringIO(request.content.decode("utf-8")),
                                       sep=";", skipinitialspace=True, na_values="EOR",
                                       parse_dates=["StartDate", "EndDate"], date_format="%Y%m%d")
        self._write_meta(self._format_meta(df))
        omsz_logger.info("Metadata updated to database")

    def _format_prev_weather(self, df: pd.DataFrame):
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.index = df['Time']
        df.drop('Time', axis=1, inplace=True)  # index creates duplicate
        df.dropna(how='all', axis=1, inplace=True)  # remove NaN columns
        return df

    def _download_prev_data(self, url: str) -> pd.DataFrame | None:
        """
        Downloads given historical/recent data at given url, gets DataFrame from csv inside a zip
        :param url: Url to ZIP
        :return: Downloaded DataFrame
        """
        omsz_logger.info(f"Requesting historical/recent data at '{url}'")
        request = req_get(url)
        if request.status_code != 200:
            omsz_logger.error(f"Historical/recent data download failed with {request.status_code} | {url}")
            return
        omsz_logger.debug(f"Historical/recent data recieved from '{url}'")

        with ZipFile(io.BytesIO(request.content), 'r') as zip_file:
            df: pd.DataFrame = pd.read_csv(zip_file.open(zip_file.namelist()[0]), comment='#',  # skip metadata of csv
                                           sep=';', skipinitialspace=True, na_values=['EOR', -999], low_memory=False,
                                           parse_dates=['Time'], date_format="%Y%m%d%H%M"
                                           )

        return self._format_prev_weather(df)

