from requests import get as req_get
from pathlib import Path
import logging
import sqlite3
from contextlib import closing
import pandas as pd
import io

omsz_logger = logging.getLogger('omsz')
omsz_logger.setLevel(logging.DEBUG)
omsz_logger.addHandler(logging.NullHandler())


class OMSZ_Downloader():
    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._con: sqlite3.Connection = None
        self._curs: sqlite3.Cursor = None

    def _db_connection(func):
        def execute(self, *args, **kwargs):
            with closing(sqlite3.connect(self._db_path)) as self._con, self._con as self._curs:
                omsz_logger.debug("Database connection opened")
                func(self, *args, **kwargs)
            omsz_logger.debug("Database connection closed")
        return execute

    @_db_connection
    def _write_meta(self, df: pd.DataFrame):
        """
        Write metadata to Database
        :param df: DataFrame to write to Database
        :return: None
        """
        df.to_sql(name="omsz_meta", con=self._con, if_exists="replace")

    def _format_meta(self, meta: pd.DataFrame):
        """
        Formats metadata
        :param meta: DataFrame containing metadata
        :return: Formatted metadata DataFrame
        """
        meta.columns = meta.columns.str.strip()
        meta.index = meta['StationNumber']
        meta.drop('StationNumber', axis=1, inplace=True)
        meta.dropna(how='all', axis=1, inplace=True)
        meta = meta[~meta.index.duplicated(keep='last')]
        return meta

    def update_meta(self):
        # Request metadata
        url = 'https://odp.met.hu/climate/observations_hungary/hourly/station_meta_auto.csv'
        omsz_logger.info("Requesting metadata")
        request = req_get(url)
        if request.status_code != 200:
            omsz_logger.error(f"Meta data download failed with {request.status_code}")
            return
        omsz_logger.debug("Meta data recieved")

        # Load data, format and write to DB
        df: pd.DataFrame = pd.read_csv(io.StringIO(request.content.decode('utf-8')),
                                       sep=';', skipinitialspace=True, na_values='EOR',
                                       parse_dates=['StartDate', 'EndDate'], date_format='%Y%m%d')
        self._write_meta(self._format_meta(df))
        omsz_logger.info("Metadata updated to database")

