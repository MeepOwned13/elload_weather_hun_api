from requests import Session
from pathlib import Path
import logging
import sqlite3
import pandas as pd
import io
from zipfile import ZipFile
import bs4
import re
from datetime import datetime
from .utils.db_connect import DatabaseConnect

omsz_downloader_logger = logging.getLogger("omsz")
omsz_downloader_logger.setLevel(logging.DEBUG)
omsz_downloader_logger.addHandler(logging.NullHandler())


class OMSZ_Downloader(DatabaseConnect):
    """
    Class to update OMSZ data inside given Database
    CALL startup_sequence() TO CREATE ALL REQUIRED TABLES
    Checking for the existence of OMSZ_meta isn't included to increase performance
    """

    def __init__(self, db_path: Path, unsafe_setup=False):
        """
        :param db_path: Path to Database
        :param unsafe_setup: turns off journal for historical/recent data inserts, massive speedup
                             but any error results in possibly corrupted data
        """
        super().__init__(db_path, omsz_downloader_logger)
        self._sess: Session = Session()
        self._RENAME: dict = {"Station Number": "StationNumber",  # Station Number
                              "StationNumber": "StationNumber",  # Station Number
                              "Time": "Time",  # Time of data
                              "r": "Prec",  # Precipitation sum
                              "t": "Temp",  # Momentary temperature
                              "ta": "AvgTemp",  # Average temperature
                              "tn": "MinTemp",  # Minimum temperature
                              "tx": "MaxTemp",  # Maximum temperature
                              "v": "View",  # Horizontal sight distance
                              "p": "Pres",  # Instrument level pressure
                              "u": "RHum",  # Relative Humidity
                              "sg": "AvgGamma",  # Average Gamma dose
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
        self.unsafe_prev_write = unsafe_setup

    def __del__(self):
        if self._con:
            # self._drop_temp()
            pass
        super().__del__()

    @DatabaseConnect._db_transaction
    def _drop_temp(self):
        self._curs_.execute("DROP TABLE IF EXISTS _temp_meta")
        self._curs_.execute("DROP TABLE IF EXISTS _temp_omsz")
        self._logger.debug("Dropped temporary tables if they existed")

    @DatabaseConnect._db_transaction
    def _create_data_table(self):
        self._curs_.execute(
            """
            CREATE TABLE IF NOT EXISTS OMSZ_data(
                Time TIMESTAMP,
                StationNumber INTEGER,
                Prec REAL,
                Temp REAL,
                AvgTemp REAL,
                MinTemp REAL,
                MaxTemp REAL,
                View REAL,
                Pres REAL,
                RHum INTEGER,
                AvgGamma REAL,
                GRad REAL,
                AvgUV REAL,
                AvgWS REAL,
                AvgWD REAL,
                MaxWS REAL,
                MaxWD REAL,
                MaxWMin INTEGER,
                MaxWSec INTEGER,
                STemp5 REAL,
                STemp10 REAL,
                STemp20 REAL,
                STemp50 REAL,
                STemp100 REAL,
                MinNSTemp REAL,
                WTemp REAL,
                PRIMARY KEY (Time, StationNumber),
                FOREIGN KEY(StationNumber) REFERENCES OMSZ_meta(StationNumber)
                )
            """
        )
        self._curs_.execute("CREATE INDEX IF NOT EXISTS OMSZ_data_station_index ON OMSZ_data(StationNumber)")
        self._curs_.execute("CREATE INDEX IF NOT EXISTS OMSZ_data_time_index ON OMSZ_data(Time)")

        self._logger.debug("Created data table if it didn't exist")

    def _df_cols_to_sql_cols(self, df: pd.DataFrame):
        """
        Convert column names to SQL viable string, needs at least 1 column
        Useful to specify columns of Tables at insertion to avoid problems with orders
        :param df: DataFrame to get columns of
        :returns: SQL compatible string of column names
        """
        cols = (df.index.name,) + tuple(df.columns)

        if len(cols) < 1:
            raise ValueError("No columns")

        # SQLite prefers " instead of ', also removing () after tuple->str
        col_str = str(cols).replace('\'', '\"')[1:-1]
        if len(cols) == 1:
            col_str = col_str[:-1]  # tuple->str leaves a ',' if it has a single element

        return col_str

    @DatabaseConnect._db_transaction
    def _write_meta(self, df: pd.DataFrame) -> None:
        """
        Write metadata to Database
        :param df: DataFrame to write to Database
        """
        self._logger.info("Starting update to metadata table")
        # StarDate and EndDate will be accessible through a VIEW named OMSZ_status
        df.loc[:, ["StartDate", "EndDate"]] = pd.NaT

        tables = self._curs_.execute("SELECT tbl_name FROM sqlite_master").fetchall()
        tables = [t[0] for t in tables]
        if "OMSZ_meta" not in tables:
            self._logger.info("Creating metadata table")
            df.to_sql(name="_temp_meta", con=self._con, if_exists='replace')
            # I want a primary key for the table
            sql = self._curs_.execute("SELECT sql FROM sqlite_master WHERE tbl_name = \"_temp_meta\"").fetchone()[0]
            sql = sql.replace("_temp_meta", "OMSZ_meta")
            sql = sql.replace("\"StationNumber\" INTEGER", "\"StationNumber\" INTEGER PRIMARY KEY")
            self._curs_.execute(sql)
            self._curs_.execute("CREATE INDEX ix_omsz_meta_StationNumber ON OMSZ_meta (StationNumber)")
            self._logger.debug("Created metadata table")
        else:
            df.to_sql(name="_temp_meta", con=self._con, if_exists='replace')

        # Copy over the data
        cols = "StationNumber, " + str(tuple(df.columns))[1:-1].replace("\'", "")
        # SQL should look like this:
        # INSERT INTO table ([cols]) SELECT [cols] FROM temp WHERE Time NOT IN (SELECT Time FROM table)
        # Watch the first set of cols need (), but the second don't, also gonna remove ' marks
        self._curs_.execute(f"INSERT INTO OMSZ_meta ({cols}) SELECT {cols} FROM _temp_meta "
                            f"WHERE StationNumber NOT IN (SELECT StationNumber FROM OMSZ_meta)")

        self._logger.info("Metadata updated to database")

    def _format_meta(self, meta: pd.DataFrame) -> pd.DataFrame:
        """
        Formats metadata
        :param meta: DataFrame containing metadata
        :returns: Formatted metadata DataFrame
        """
        meta.columns = meta.columns.str.strip()  # remove trailing whitespace
        meta["StationName"] = meta["StationName"].str.strip()
        meta["RegioName"] = meta["RegioName"].str.strip()
        meta.set_index("StationNumber", drop=True, inplace=True)
        meta.dropna(how="all", axis=1, inplace=True)
        meta = meta[~meta.index.duplicated(keep="last")]  # duplicates
        return meta.copy()

    def update_meta(self) -> None:
        """
        Downloads metadata and writes it to sqlite Database
        """
        # Request metadata
        url = "https://odp.met.hu/climate/observations_hungary/hourly/station_meta_auto.csv"
        self._logger.info(f"Requesting metadata at '{url}'")
        request = self._sess.get(url)
        if request.status_code != 200:
            self._logger.error(f"Meta data download failed with {request.status_code} | {url}")
            return
        self._logger.debug(f"Meta data recieved from '{url}'")

        # Load data, format and write to DB
        df: pd.DataFrame = pd.read_csv(io.StringIO(request.content.decode("utf-8")),
                                       sep=";", skipinitialspace=True, na_values="EOR",
                                       parse_dates=["StartDate", "EndDate"], date_format="%Y%m%d")
        self._write_meta(self._format_meta(df))

    def _format_prev_weather(self, df: pd.DataFrame):
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.drop(columns=[col for col in df if col not in self._RENAME.keys()], inplace=True)
        df.rename(columns=self._RENAME, inplace=True)
        df.set_index("Time", drop=True, inplace=True)  # Time is stored in UTC
        return df

    def _download_prev_data(self, url: str) -> pd.DataFrame | None:
        """
        Downloads given historical/recent data at given url, gets DataFrame from csv inside a zip
        :param url: Url to ZIP
        :returns: Downloaded DataFrame
        """
        self._logger.debug(f"Requesting historical/recent data at '{url}'")
        request = self._sess.get(url)
        if request.status_code != 200:
            self._logger.error(f"Historical/recent data download failed with {request.status_code} | {url}")
            return
        self._logger.debug(f"Historical/recent data recieved from '{url}'")

        with ZipFile(io.BytesIO(request.content), 'r') as zip_file:
            df: pd.DataFrame = pd.read_csv(zip_file.open(zip_file.namelist()[0]), comment='#',  # skip metadata of csv
                                           sep=';', skipinitialspace=True, na_values=['EOR', -999], low_memory=False,
                                           parse_dates=['Time'], date_format="%Y%m%d%H%M"
                                           )

        return self._format_prev_weather(df)

    @DatabaseConnect._db_transaction
    def _filter_stations_from_url(self, urls: list[str]) -> list[str]:
        """
        Filter given urls/strings where they contain station numbers for stations we have metadata for
        :param urls: Urls/strings to filter
        :returns: Filtered urls or empty list if Database interaction failed
        """
        # get all stations from metadata
        try:
            stations = self._curs_.execute("SELECT stationnumber FROM OMSZ_meta").fetchall()
            stations = [s[0] for s in stations]  # remove them from tuples
        except sqlite3.OperationalError:
            self._logger.error(
                "Station filtering failed, can't access Database or OMSZ_meta table doesn't exist")
            return []

        # filter stations we have metadata for
        regex_meta = re.compile(r".*_(\d{5})_.*")
        regex_hist = re.compile(fr".*_{datetime.today().year-1}1231_.*")
        regex_rec = re.compile(r".*akt.*")
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

    def _get_weather_downloads(self, url: str, current: bool = False) -> list[str]:
        """
        Gather urls at a given site that contain download links for weather data
        :param url: Site to search
        :param current: Extracting current weather data or not? Used for filtering
        :returns: List of download urls
        """
        self._logger.info(f"Requesting weather data urls at '{url}'")
        request = self._sess.get(url)
        if request.status_code != 200:
            self._logger.error(f"Weather data url request failed with {request.status_code}")
            return []

        soup = bs4.BeautifulSoup(request.text, 'html.parser')
        file_downloads = soup.find_all('a')  # find links

        regex = re.compile(r'.*\.zip')
        # get all hrefs, and filter recurring ones (most were acquired twice)
        file_downloads = list(set([link.get('href').strip()
                              for link in file_downloads if regex.match(link.get('href'))]))

        self._logger.info(f"Weather data urls extracted from '{url}'")

        # metadata only contains info about stations currently active
        # we only want data from stations we had the metadata from
        if current:
            # This filter is short, so I will leave it in this function
            file_downloads = [file for file in file_downloads if file.find("LATEST") == -1]
            # LATEST leads to duplicates here, will use it in a different function
        else:
            file_downloads = self._filter_stations_from_url(file_downloads)

        return [f"{url}{file}" for file in file_downloads]

    @DatabaseConnect._db_transaction
    def _write_prev_weather_data(self, df: pd.DataFrame) -> None:
        """
        Write historical/recent weather data to corresponding Table
        :param df: DataFrame to use
        :returns: None
        """
        # Check if DataFrame is empty or only has it's index
        if df.empty or len(tuple(df.columns)) == 0:
            self._logger.warning("Table writing was called with an empty DataFrame")
            return

        station = df["StationNumber"].iloc[0]
        self._logger.debug(f"Starting historical/recent write with {station} to OMSZ_data")

        # Specifying column names to be safe from different orderings of columns
        cols = self._df_cols_to_sql_cols(df)

        df.to_sql(name="_temp_omsz", con=self._con, if_exists='replace')
        if self.unsafe_prev_write:
            self._curs_.execute("PRAGMA journal_mode = OFF")

        self._curs_.execute(f"INSERT INTO OMSZ_data ({cols}) SELECT {cols} FROM _temp_omsz "
                            f"WHERE Time NOT IN (SELECT Time FROM OMSZ_data WHERE StationNumber = {station})")

        if self.unsafe_prev_write:
            self._curs_.execute("PRAGMA journal_mode = DEFAULT")

        self._update_start_end_dates(station)

        self._logger.info(f"Updated OMSZ_data with historical/recent data for {station}")

    @DatabaseConnect._db_transaction
    def _is_hist_needed(self, url: str) -> bool:
        """
        Checks if given historical url would contain data we need
        :param url: url to check
        :returns: Should this data be downloaded?
        """
        # Technically, urls are pre-filtered to contain stations which are still active,
        # but this method will check the year for safety
        last_year = datetime.today().year - 1
        regex = re.compile(fr".*_(\d{{5}})_.*{last_year}1231_.*")
        match = regex.match(url)
        if not match:
            return False

        station = match.group(1)
        # Historical csv-s contain data up to lastyear-12-31 23:50:00 UTC
        # Need to request it, if no EndDate is specified (meaning no data yet) or
        # The EndDate is from before this year => res.fetchall() will return a non-empty list
        res = self._curs_.execute(f"SELECT * FROM OMSZ_meta "
                                  f"WHERE StationNumber = {station} AND "
                                  f"(EndDate IS NULL OR EndDate < datetime(\"{last_year}-12-31 23:50:00\"))"
                                  )

        return bool(res.fetchone())

    def update_hist_weather_data(self) -> None:
        """
        Update historical weather data
        :returns: None
        """
        self._logger.info("Downloading and updating with historical weather data")

        hist_urls = self._get_weather_downloads(
            "https://odp.met.hu/climate/observations_hungary/10_minutes/historical/")
        for url in hist_urls:
            if self._is_hist_needed(url):
                data = self._download_prev_data(url)
                if data is None:
                    continue
                self._write_prev_weather_data(data)
            else:
                self._logger.debug(f"Historical data not needed at {url}")

        self._logger.info("Finished downloading and updating with historical weather data")

    @DatabaseConnect._db_transaction
    def _is_rec_needed(self, url: str) -> bool:
        """
        Checks if given historical url would contain data we need
        :param url: url to check
        :returns: Should this data be downloaded?
        """
        # Technically, urls are pre-filtered to contain stations which are still active,
        # but this method will check the year for safety
        check = pd.Timestamp.now("UTC").tz_localize(None) - pd.Timedelta(days=1)
        regex = re.compile(r".*_(\d{5})_.*")
        match = regex.match(url)
        if not match:
            return False

        station = match.group(1)
        # Recent csv-s contain data up to 1 day (24 hours) ago
        # Need to request it, if no EndDate is specified (meaning no data yet) or
        # The EndDate is from before this year => res.fetchall() will return a non-empty list
        res = self._curs_.execute(f"SELECT * FROM OMSZ_meta "
                                  f"WHERE StationNumber = {station} AND "
                                  f"(EndDate IS NULL OR EndDate <= datetime(\"{check.strftime('%Y-%m-%d %H-%M-%S')}\"))"
                                  )

        return bool(res.fetchone())

    def update_rec_weather_data(self):
        """
        Update recent weather data
        :returns: Any updates happened?
        """
        self._logger.info("Downloading and updating with recent weather data")

        updated = False
        rec_urls = self._get_weather_downloads("https://odp.met.hu/climate/observations_hungary/10_minutes/recent/")
        for url in rec_urls:
            if self._is_rec_needed(url):
                data = self._download_prev_data(url)
                if data is None or data.empty or len(tuple(data.columns)) == 0:
                    continue
                self._write_prev_weather_data(data)
                updated = True
            else:
                self._logger.debug(f"Recent data not needed at {url}")

        self._logger.info("Finished downloading and updating with recent weather data")

        return updated

    def _format_past24h_weather(self, df: pd.DataFrame):
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.drop(columns=[col for col in df if col not in self._RENAME.keys()], inplace=True)
        df.rename(columns=self._RENAME, inplace=True)
        # Time is in UTC
        df.set_index("Time", drop=True, inplace=True)
        return df

    def _download_curr_data(self, url: str) -> pd.DataFrame | None:
        """
        Downloads given current data at given url, gets DataFrame from csv inside a zip
        :param url: Url to ZIP
        :returns: Downloaded DataFrame
        """
        self._logger.debug(f"Requesting current data at '{url}'")
        request = self._sess.get(url)
        if request.status_code != 200:
            self._logger.error(f"Current data download failed with {request.status_code} | {url}")
            return
        self._logger.debug(f"Current data recieved from '{url}'")

        with ZipFile(io.BytesIO(request.content), 'r') as zip_file:
            df: pd.DataFrame = pd.read_csv(zip_file.open(zip_file.namelist()[0]), comment='#',  # skip metadata of csv
                                           sep=';', skipinitialspace=True, na_values=['EOR', -999], low_memory=False,
                                           parse_dates=['Time'], date_format="%Y%m%d%H%M"
                                           )

        return self._format_past24h_weather(df)

    @DatabaseConnect._assert_transaction
    def _write_curr_weather_data(self, df: pd.DataFrame) -> None:
        """
        Write current weather data to corresponding Tables
        THIS FUNCTION ASSUMES THERE IS AN ONGOING TRANSACTION
        :param df: DataFrame to use
        :returns: None
        """
        # Check if DataFrame is empty or only has it's index
        if df.empty or len(tuple(df.columns)) == 0:
            self._logger.warning("Table writing was called with an empty DataFrame")
            return

        df.to_sql(name="_temp_omsz", con=self._con, if_exists='replace')

        # Specifying column names to be safe from different orderings of SQL columns
        cols = self._df_cols_to_sql_cols(df)

        # Ignoring if an integrity conflict would happen (PRIMARY KEY -> UNIQUE)
        # Ignoring seemed slower, but for this little data, it's not noticable
        self._curs_.execute(f"INSERT OR IGNORE INTO OMSZ_data ({cols}) SELECT {cols} FROM _temp_omsz")

        # Doing the EndDate update here since it is much faster than trying to find max (+min) in 100_000_000+ lines
        for station in [s[0] for s in self._curs_.execute("SELECT StationNumber FROM OMSZ_meta")]:
            end = self._curs_.execute(
                f"SELECT MAX(Time) FROM _temp_omsz WHERE StationNumber = {station}").fetchone()[0]
            if end:  # not None
                self._curs_.execute(f"UPDATE OMSZ_meta SET "
                                    f"EndDate = datetime(\"{end}\") WHERE StationNumber = {station} AND "
                                    f"EndDate < datetime(\"{end}\")")

    @DatabaseConnect._db_transaction
    def update_past24h_weather_data(self) -> None:
        """
        Updates weather data with the last 24 hours
        :returns: None
        """
        # WHILE UPDATING, THIS FUNCTION DOES A SINGLE TRANSACTION,
        # THIS IS TO PREVENT PROBLEMS ARISING FROM NOT INSERTING EACH TIME IN AN ORDERED MANNER
        self._logger.info("Downloading and updating with the past 24h of weather data")

        curr_urls = self._get_weather_downloads(
            "https://odp.met.hu/weather/weather_reports/synoptic/hungary/10_minutes/csv/", current=True)

        for url in curr_urls:
            data = self._download_curr_data(url)
            self._logger.debug(f"Past 24h of weather data recieved from '{url}'")
            if data is None:
                continue
            self._write_curr_weather_data(data)

        self._logger.info("Finished downloading and updating with the past 24h of weather data")

    @DatabaseConnect._db_transaction
    def update_curr_weather_data(self) -> None:
        """
        Updates weather data with the current LATEST entries
        Useful if data is being requested every time there are new entries
        :returns: None
        """
        # WHILE UPDATING, THIS FUNCTION DOES A SINGLE TRANSACTION,
        # THIS IS BECAUSE FUNCTIONS USED EXPECT AN ONGOING TRANSACTION
        # see also: self.update_past24h_weather_data()
        self._logger.info("Downloading and updating with the most recent weather data")

        data = self._download_curr_data(
            "https://odp.met.hu/weather/weather_reports/synoptic/hungary/10_minutes/csv/HABP_10M_SYNOP_LATEST.csv.zip")
        self._logger.info("Most recent weather data recieved")
        self._write_curr_weather_data(data)

        self._logger.info("Finished downloading and updating with the most recent weather data")

    @DatabaseConnect._assert_transaction
    def _update_start_end_dates(self, station=None):
        """
        Update start and end date in meta
        :param station: if specified will only perform update for corresponding station
        """
        stations = [station] if station else [s[0] for s in self._curs_.execute("SELECT StationNumber FROM OMSZ_meta")]
        for station in stations:
            start, end = self._curs_.execute(
                f"SELECT MIN(Time), MAX(Time) FROM OMSZ_data WHERE StationNumber = {station}").fetchone()
            if start and end:  # not None
                self._curs_.execute(f"UPDATE OMSZ_meta SET StartDate = datetime(\"{start}\"), "
                                    f"EndDate = datetime(\"{end}\") WHERE StationNumber = {station}")

        self._logger.info(f"Updated start and end dates for {station if len(stations) == 1 else 'all stations'}")

    @DatabaseConnect._db_transaction
    def _get_max_end_date(self) -> pd.Timestamp | None:
        """
        Gets the maximum of EndDate in omsz_meta
        :returns: pandas.Timestamp for max end date
        """
        date = self._curs_.execute("SELECT MAX(EndDate) FROM OMSZ_meta").fetchone()[0]
        return pd.to_datetime(date, format="%Y-%m-%d %H:%M:%S")

    def choose_curr_update(self) -> bool:
        """
        Chooses to do a current weather data update if necessary
        This function assumes that hist/recent data are already updated
        :returns: did an update happen?
        """
        now: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
        end: pd.Timestamp = self._get_max_end_date()
        if now > (end + pd.Timedelta(minutes=20)):
            if now < (end + pd.Timedelta(minutes=30)):
                self.update_curr_weather_data()
            else:
                self.update_past24h_weather_data()
            return True
        return False

    def startup_sequence(self) -> None:
        """
        Calls meta, historical/recent and past24h updates
        :returns: None
        """
        # Order of operations justified in comments
        self._create_data_table()
        self.update_meta()
        # After tables are created it's time to update
        # Starting with historical since it doesn't affect any of the following ones and is a long running operation
        self.update_hist_weather_data()
        # Updating past24h first because data from 24h ago will be there
        self.update_past24h_weather_data()
        # Recent comes next (current year)
        # Doing this after the past24h ensures that there are no gaps happening at the t-24h mark
        # (Theoretically reverse order could result in missing t-24h if it passes a 10 min mark during it)
        if self.update_rec_weather_data():
            # Past24h again to ensure we have the most recent data before starting if any updates happened with recent
            # Recent update could result in passingMAX a 10 min mark
            self.update_past24h_weather_data()

