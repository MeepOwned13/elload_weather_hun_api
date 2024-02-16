from requests import Session
from pathlib import Path
import logging
import sqlite3
from contextlib import closing
import pandas as pd
import io
from zipfile import ZipFile
import bs4
import re
from datetime import datetime

omsz_logger = logging.getLogger("omsz")
omsz_logger.setLevel(logging.DEBUG)
omsz_logger.addHandler(logging.NullHandler())


class OMSZ_Downloader():
    def __init__(self, db_path: Path):
        self._db_path: Path = db_path
        self._con: sqlite3.Connection = None
        self._curs: sqlite3.Cursor = None
        self._sess: Session = Session()
        self._RENAME: dict = {"Station Number": "StationNumber",  # Station Number
                              "Time": "Time",  # Time of data, transformed to UTC+1
                              "r": "Prec",  # Precipitation sum
                              "t": "Temp",  # Momentary temperature
                              "ta": "AvgTemp",  # Average temperature
                              "tn": "MinTemp",  # Minimum temperature
                              "tx": "MaxTemp",  # Maximum temperature
                              "v": "View",  # Horizontal sight distance
                              "p": "Pres",  # Instrument level pressure
                              "u": "RHum",  # Relative Humidity
                              "sg": "AvgGamma",  # Average Gamma does
                              "sr": "GRad",  # Global Radiation
                              "suv": "AvgUV",  # Average UV radiation
                              "fs": "AvgWS",  # Average Wind Speed
                              "fsd": "AvgWD",  # Average Wind Direction
                              "fx": "MaxWS",  # Maximum Wind gust Speed
                              "fxd": "MaxWD",  # Maximum Wind gust Direction
                              "fxm": "MaxWMin",  # Maximum Wind gust Minute
                              "fxs": "MaxWSec",  # Maximum Wind gust Second
                              "et5": "STemp5",  # Soil Temperature at 5cm
                              "et10": "STemp10",  # Soil Temperature at 10cm
                              "et20": "STemp20",  # Soil Temperature at 20cm
                              "et50": "STemp50",  # Soil Temperature at 50cm
                              "et100": "STemp100",  # Soil Temperature at 100cm
                              "tsn": "MinNSTemp",  # Minimum Near-Surface Temperature
                              "tviz": "WTemp",  # Water Temperature
                              }

    def _db_connection(func):
        def execute(self, *args, **kwargs):
            with closing(sqlite3.connect(self._db_path, timeout=60)) as self._con, self._con as self._curs:
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
        omsz_logger.info("Starting update to metadata table")

        tables = self._curs.execute("SELECT tbl_name FROM sqlite_master").fetchall()
        tables = [t[0] for t in tables]
        if "omsz_meta" not in tables:
            omsz_logger.info("Creating metadata table")
            df.to_sql(name="_temp_meta", con=self._con, if_exists='replace')
            # I want a primary key for the table
            sql = self._curs.execute("SELECT sql FROM sqlite_master WHERE tbl_name = \"_temp_meta\"").fetchone()[0]
            sql = sql.replace("_temp_meta", "omsz_meta")
            sql = sql.replace("\"StationNumber\" INTEGER", "\"StationNumber\" INTEGER PRIMARY KEY")
            self._curs.execute(sql)
            self._curs.execute("CREATE INDEX ix_omsz_meta_StationNumber ON omsz_meta (StationNumber)")
            omsz_logger.debug("Created metadata table")
        else:
            df.to_sql(name="_temp_meta", con=self._con, if_exists='replace')

        # Copy over the data
        cols = "StationNumber, " + str(tuple(df.columns))[1:-1].replace("\'", "")
        # SQL should look like this:
        # INSERT INTO table ([cols]) SELECT [cols] FROM temp WHERE Time NOT IN (SELECT Time FROM table)
        # Watch the first set of cols need (), but the second don't, also gonna remove ' marks
        self._curs.execute(f"INSERT INTO omsz_meta ({cols}) SELECT {cols} FROM _temp_meta "
                           f"WHERE StationNumber NOT IN (SELECT StationNumber FROM omsz_meta)")

        omsz_logger.info("Metadata updated to database")

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
        request = self._sess.get(url)
        if request.status_code != 200:
            omsz_logger.error(f"Meta data download failed with {request.status_code} | {url}")
            return
        omsz_logger.debug(f"Meta data recieved from '{url}'")

        # Load data, format and write to DB
        df: pd.DataFrame = pd.read_csv(io.StringIO(request.content.decode("utf-8")),
                                       sep=";", skipinitialspace=True, na_values="EOR",
                                       parse_dates=["StartDate", "EndDate"], date_format="%Y%m%d")
        self._write_meta(self._format_meta(df))

    def _format_prev_weather(self, df: pd.DataFrame):
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.rename(columns=self._RENAME, inplace=True)
        df['Time'] += pd.Timedelta(hours=1)  # move to UTC+1
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
        request = self._sess.get(url)
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

    @_db_connection
    def _filter_stations_from_url(self, urls: list[str]) -> list[str]:
        """
        Filter given urls/strings where they contain station numbers for stations we have metadata for
        :param urls: Urls/strings to filter
        :return: Filtered urls or empty list if Database interaction failed
        """
        # get all stations from metadata
        try:
            stations = self._curs.execute("SELECT stationnumber FROM omsz_meta").fetchall()
            stations = [s[0] for s in stations]  # remove them from tuples
        except sqlite3.OperationalError:
            omsz_logger.error("Station filtering failed, can't access Database or omsz_meta table doesn't exist")
            return []

        # filter stations we have metadata for
        regex_meta = re.compile(r'.*_(\d{5})_.*')
        regex_hist = re.compile(fr'.*_{datetime.today().year-1}1231_.*')
        regex_rec = re.compile(r'.*akt.*')
        filtered = []
        for url in urls:
            # first check filters ones we have metadata for and
            # also filters recent data where station number is not provided if the csv doesn't contain data up to today
            # second filters ones that don't go until the end of last year for historical (not needed for recent)
            meta = regex_meta.match(url)
            station = int(meta.group(1)) if meta else -1
            if station in stations and (regex_hist.match(url) or regex_rec.match(url)):
                filtered.append(url)

        return filtered

    def _get_prev_downloads(self, url: str) -> list[str]:
        """
        Gather urls at a given site that contain download links for historical/recent data
        :param url: Site to search
        :return: List of download urls
        """
        omsz_logger.info(f"Requesting historical/recent data urls at '{url}'")
        request = self._sess.get(url)
        if request.status_code != 200:
            omsz_logger.error(f"Historical/recent data url request failed with {request.status_code}")
            return []

        soup = bs4.BeautifulSoup(request.text, 'html.parser')
        file_downloads = soup.find_all('a')  # find links

        regex = re.compile(r'.*\.zip')
        # get all hrefs, and filter recurring ones (most were acquired twice)
        file_downloads = list(set([link.get('href').strip()
                              for link in file_downloads if regex.match(link.get('href'))]))

        omsz_logger.info(f"Historical/recent data urls extracted from '{url}'")

        # metadata only contains info about stations currently active
        # we only want data from stations we had the metadata from
        file_downloads = self._filter_stations_from_url(file_downloads)

        return [f"{url}{file}" for file in file_downloads]

    @_db_connection
    def _write_prev_weather(self, df: pd.DataFrame) -> None:
        """
        Write historical/recent weather data to corresponding Table
        :param df: DataFrame to use
        :return: None
        """
        # Check if DataFrame is empty or only has it's index
        if df.empty or len(tuple(df.columns)) == 0:
            omsz_logger.warning("Table writing was called with an empty DataFrame")
            return

        station = df["StationNumber"].iloc[0]
        df.drop(columns="StationNumber", inplace=True)
        table_name = f"OMSZ_{station}"

        omsz_logger.info(f"Starting write to table {table_name}")
        tables = self._curs.execute("SELECT tbl_name FROM sqlite_master").fetchall()
        tables = [t[0] for t in tables]
        if table_name not in tables:
            omsz_logger.info(f"Creating new table {table_name}")
            df.to_sql(name="_temp_omsz", con=self._con, if_exists='replace')
            # I want a primary key for the table
            sql = self._curs.execute("SELECT sql FROM sqlite_master WHERE tbl_name = \"_temp_omsz\"").fetchone()[0]
            sql = sql.replace("_temp_omsz", table_name)
            sql = sql.replace("\"Time\" TIMESTAMP", "\"Time\" TIMESTAMP PRIMARY KEY")
            self._curs.execute(sql)
            self._curs.execute(f"CREATE INDEX ix_{table_name}_Time ON {table_name} (Time)")
            omsz_logger.debug(f"Created new table {table_name}")
        else:
            # Idea: create temp table and insert values missing into the actual table
            omsz_logger.info(f"Table {table_name} already exists, inserting new values")
            df.to_sql(name="_temp_omsz", con=self._con, if_exists="replace")

        # Tuple->String in Python leaves a single ',' if the tuple has 1 element
        df_cols = tuple(df.columns)
        if len(df_cols) == 1:
            cols = "Time, " + str(tuple(df.columns))[1:-2].replace("\'", "")
        else:
            cols = "Time, " + str(tuple(df.columns))[1:-1].replace("\'", "")

        # SQL should look like this:
        # INSERT INTO table ([cols]) SELECT [cols] FROM temp WHERE Time NOT IN (SELECT Time FROM table)
        # Watch the first set of cols need (), but the second don't, also gonna remove ' marks
        self._curs.execute(f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM _temp_omsz "
                           f"WHERE Time NOT IN (SELECT Time FROM {table_name})")

        omsz_logger.info(f"Updated {table_name}")

    def update_prev_weather_data(self):
        omsz_logger.info("Checking if prev weather data download is necessary")
        # TODO: implement check (also check if we need historical at all)

        omsz_logger.info("Donwloading prev weather data")

        for horizon in ("historical", "recent"):
            urls = self._get_prev_downloads(f"https://odp.met.hu/climate/observations_hungary/10_minutes/{horizon}/")
            for url in urls:
                self._write_prev_weather(self._download_prev_data(url))

        omsz_logger.info("Downloaded prev weather data")

