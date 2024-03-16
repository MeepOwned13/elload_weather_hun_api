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
    MAKE SURE TO HAVE OMSZ_META TABLE OR RUN UPDATE_META() BEFORE CALLING OTHER FUNCTIONS
    Checking for the existence of OMSZ_meta isn't included to increase performance
    """

    def __init__(self, db_path: Path):
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

    def __del__(self):
        if self._con:
            self._drop_temp()
        super().__del__()

    @DatabaseConnect._db_transaction
    def _drop_temp(self):
        self._curs_.execute("DROP TABLE IF EXISTS _temp_meta")
        self._curs_.execute("DROP TABLE IF EXISTS _temp_omsz")
        self._logger.debug("Dropped temporary tables if they existed")

    @DatabaseConnect._db_transaction
    def _write_meta(self, df: pd.DataFrame) -> None:
        """
        Write metadata to Database
        :param df: DataFrame to write to Database
        """
        self._logger.info("Starting update to metadata table")
        # StarDate and EndDate is maintained based on actual, inserted data
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
        return meta

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
        df.rename(columns=self._RENAME, inplace=True)
        df.set_index("Time", drop=True, inplace=True)  # Time is stored in UTC
        df.dropna(how='all', axis=1, inplace=True)  # remove NaN columns
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

    @DatabaseConnect._assert_transaction
    def _update_end_date_meta(self, station: int) -> None:
        """
        Updates the EndDate inside OMSZ_meta for given station
        THIS FUNCTION ASSUMES THERE IS AN ONGOING TRANSACTION
        :param station: Station Number to update
        :returns: None
        """
        end_date = self._curs_.execute(f"SELECT MAX(Time) FROM OMSZ_{station}").fetchone()[0]
        self._curs_.execute(f"UPDATE OMSZ_meta SET EndDate = datetime(\"{end_date}\") "
                            f"WHERE StationNumber = {station} AND "
                            f"(EndDate IS NULL OR EndDate < datetime(\"{end_date}\"))"
                            )

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
        df.drop(columns="StationNumber", inplace=True)
        table_name = f"OMSZ_{station}"

        self._logger.info(f"Starting write to table {table_name}")
        exists = self._curs_.execute(
            f"SELECT name FROM sqlite_master WHERE type=\"table\" AND name=\"{table_name}\"").fetchone()
        if not exists:
            self._logger.info(f"Creating new table {table_name}")
            df.to_sql(name="_temp_omsz", con=self._con, if_exists='replace')
            # I want a primary key for the table
            sql = self._curs_.execute("SELECT sql FROM sqlite_master WHERE tbl_name = \"_temp_omsz\"").fetchone()[0]
            sql = sql.replace("_temp_omsz", table_name)
            sql = sql.replace("\"Time\" TIMESTAMP", "\"Time\" TIMESTAMP PRIMARY KEY")
            self._curs_.execute(sql)
            self._curs_.execute(f"CREATE INDEX ix_{table_name}_Time ON {table_name} (Time)")
            self._logger.debug(f"Created new table {table_name}")
        else:
            # Idea: create temp table and insert values missing into the actual table
            self._logger.info(f"Table {table_name} already exists, inserting new values")
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
        self._curs_.execute(f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM _temp_omsz "
                            f"WHERE Time NOT IN (SELECT Time FROM {table_name})")

        start_date = self._curs_.execute(f"SELECT MIN(Time) FROM OMSZ_{station}").fetchone()[0]
        self._curs_.execute(f"UPDATE OMSZ_meta SET StartDate = datetime(\"{start_date}\") "
                            f"WHERE StationNumber = {station} AND "
                            f"(StartDate IS NULL OR StartDate > datetime(\"{start_date}\"))"
                            )
        self._update_end_date_meta(station)

        self._logger.info(
            f"Updated {table_name}, updated StartDate and EndDate in metadata for StationNumber {station}")

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

        return bool(res.fetchall())

    def update_prev_weather_data(self) -> None:
        """
        Update historical/recent weather data
        :returns: None
        """
        # Historical
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

        # Recent
        self._logger.info("Downloading and updating with recent weather data")

        rec_urls = self._get_weather_downloads("https://odp.met.hu/climate/observations_hungary/10_minutes/recent/")
        for url in rec_urls:
            data = self._download_prev_data(url)
            if data is None:
                continue
            self._write_prev_weather_data(data)

        self._logger.info("Finished downloading and updating with recent weather data")

    def _format_past24h_weather(self, df: pd.DataFrame):
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.drop(["StationName", "Latitude", "Longitude", "Elevation"], axis="columns", inplace=True)
        df.rename(columns=self._RENAME, inplace=True)
        df.dropna(how='all', axis=1, inplace=True)  # remove NaN columns
        # We don't need indexing, only going to iterate this DataFrame once
        # Time is in UTC
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

    def _insert_curr_weather_row(self, ser: pd.Series) -> None:
        """
        Inserts current weather data row to corresponding Table
        This function assumes there is an ongoing transaction
        :param ser: Series (row) to use
        :returns: None
        """
        station = ser["StationNumber"]
        # Check if the time was already inserted into the table
        exists = self._curs_.execute(f"SELECT Time FROM OMSZ_{station} "
                                     f"WHERE Time = datetime(\"{ser['Time']}\")").fetchone()
        if exists:
            return

        cols = self._curs_.execute(f"SELECT name FROM PRAGMA_TABLE_INFO(\"OMSZ_{station}\")").fetchall()
        # Extract query result and remove NaN values,
        # also remove Time column because we have to handle datetime conversion
        cols = [c[0] for c in cols if not pd.isna(ser[c[0]]) and c[0] != "Time"]
        val_str = tuple(ser[cols].astype(str))

        # Formatting the sql command, not using (?,?,?) because amount of columns change from query to query
        if len(cols) == 1:
            col_str = str(cols)[1:-2].replace("\'", "")
            val_str = str(val_str)[1:-2].replace("\'", "")
        else:
            col_str = str(cols)[1:-1].replace("\'", "")
            val_str = str(val_str)[1:-1].replace("\'", "")

        self._curs_.execute(f"INSERT INTO OMSZ_{station} (Time, {col_str}) "
                            f"VALUES(datetime(\"{ser['Time']}\"), {val_str})")

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

        for _, row in df.iterrows():
            self._insert_curr_weather_row(row)

        stations = self._curs_.execute("SELECT StationNumber FROM OMSZ_meta "
                                       "WHERE StartDate IS NOT NULL AND EndDate IS NOT NULL").fetchall()
        for station in stations:
            station = station[0]  # results are always tuples (inside of a list)
            self._update_end_date_meta(station)

        self._logger.info("Updated EndDate in OMSZ_meta for all stations")

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
            self._logger.info(f"Past 24h of weather data recieved from '{url}'")
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

    @DatabaseConnect._db_transaction
    def _get_max_end_date(self) -> pd.Timestamp | None:
        """
        Gets the maximum of EndDate in omsz_meta
        :returns: pandas.Timestamp for max end date
        """
        date = self._curs_.execute("SELECT MAX(EndDate) FROM omsz_meta").fetchone()[0]
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
        self.update_meta()
        self.update_prev_weather_data()
        self.update_past24h_weather_data()


