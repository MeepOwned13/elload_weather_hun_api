from requests import Session
import logging
import pandas as pd
import io
from zipfile import ZipFile
import bs4
import re
from datetime import datetime
from .utils.db_connect import DatabaseConnect
from copy import copy

omsz_downloader_logger = logging.getLogger("omsz")
omsz_downloader_logger.setLevel(logging.DEBUG)
omsz_downloader_logger.addHandler(logging.NullHandler())


class OMSZDownloader(DatabaseConnect):
    """
    Class to update OMSZ data inside given Database
    CALL startup_sequence() TO CREATE ALL REQUIRED TABLES
    Checking for the existence of tables isn't included to increase performance
    """

    def __init__(self, db_connect_info: dict):
        """
        :param db_connect_info: connection info for MySQL connector
        """
        super().__init__(db_connect_info, omsz_downloader_logger)
        self._sess: Session = Session()
        rename_and_unit = [
            ("Station Number", "StationNumber", "id"),  # Station Number
            ("StationNumber", "StationNumber", "id"),  # Station Number
            ("Time", "Time", "datetime"),  # Time of data
            ("r", "Prec", "mm"),  # Precipitation sum
            ("t", "Temp", "°C"),  # Momentary temperature
            ("ta", "AvgTemp", "°C"),  # Average temperature
            ("tn", "MinTemp", "°C"),  # Minimum temperature
            ("tx", "MaxTemp", "°C"),  # Maximum temperature
            ("v", "View", "m"),  # Horizontal sight distance
            ("p", "Pres", "hPa"),  # Instrument level pressure
            ("u", "RHum", "%"),  # Relative Humidity
            ("sg", "AvgGamma", "nSv/h"),  # Average Gamma dose
            ("sr", "GRad", "W/m²"),  # Global Radiation
            ("suv", "AvgUV", "MED/h"),  # Average UV radiation
            ("fs", "AvgWS", "m/s"),  # Average Wind Speed
            ("fsd", "AvgWD", "°"),  # Average Wind Direction
            ("fx", "MaxWS", "m/s"),  # Maximum Wind gust Speed
            ("fxd", "MaxWD", "°"),  # Maximum Wind gust Direction
            ("fxm", "MaxWMin", "'"),  # Maximum Wind gust Angle Minute
            ("fxs", "MaxWSec", "''"),  # Maximum Wind gust Angle Second
            ("et5", "STemp5", "°C"),  # Soil Temperature at 5cm
            ("et10", "STemp10", "°C"),  # Soil Temperature at 10cm
            ("et20", "STemp20", "°C"),  # Soil Temperature at 20cm
            ("et50", "STemp50", "°C"),  # Soil Temperature at 50cm
            ("et100", "STemp100", "°C"),  # Soil Temperature at 100cm
            ("tsn", "MinNSTemp", "°C"),  # Minimum Near-Surface Temperature
            ("tviz", "WTemp", "°C"),  # Water Temperature

        ]
        self._RENAME: dict = {orig: new for orig, new, _ in rename_and_unit}
        self._UNITS: dict = {name: unit for _, name, unit in rename_and_unit}

    @property
    def units(self) -> dict:
        """Get the units for column names."""
        return copy(self._UNITS)

    def __del__(self):
        super().__del__()

    @DatabaseConnect._db_transaction
    def _create_tables_views(self) -> None:
        """
        Creates necessary data, meta table and status view
        """
        self._curs.execute(
            """
            CREATE TABLE IF NOT EXISTS OMSZ_meta(
                StationNumber INTEGER PRIMARY KEY,
                Latitude REAL,
                Longitude REAL,
                Elevation REAL,
                StationName TEXT,
                RegioName TEXT
                )
            """
        )
        # PRIMARY KEYs are always indexed

        self._curs.execute(
            """
            CREATE TABLE IF NOT EXISTS OMSZ_data(
                Time DATETIME,
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
                PRIMARY KEY (StationNumber, Time),
                FOREIGN KEY (StationNumber) REFERENCES OMSZ_meta(StationNumber),
                INDEX OMSZ_data_time_index (Time) USING BTREE
            )
            """
        )
        # PRIMARY KEYs are always indexed

        # View to get Start and End Dates for each station along with meta info
        self._curs.execute(
            """
            CREATE OR REPLACE VIEW OMSZ_status AS
            SELECT OMSZ_meta.StationNumber StationNumber, StartDate, EndDate,
                   Latitude, Longitude, Elevation, StationName, RegioName
            FROM OMSZ_meta LEFT JOIN (
                SELECT StationNumber, MIN(Time) StartDate, MAX(Time) EndDate from OMSZ_data GROUP BY StationNumber
                ) AS StartsEnds ON OMSZ_meta.StationNumber = StartsEnds.StationNumber
            """
        )

        self._logger.info("Created tables/views that didn't exist")

    @DatabaseConnect._db_transaction
    def _write_meta(self, df: pd.DataFrame) -> None:
        """
        Write metadata to Database
        :param df: DataFrame to write to Database
        """
        self._logger.info("Starting update to metadata table")
        # StarDate and EndDate will be accessible through a VIEW named OMSZ_status
        df.drop(columns=["StartDate", "EndDate"], inplace=True)
        self._df_to_sql(df, "OMSZ_meta")

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
        Downloads metadata and writes it to Database
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

    def _format_prev_weather(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.drop(columns=[col for col in df if col not in self._RENAME.keys()], inplace=True)
        df.rename(columns=self._RENAME, inplace=True)
        df.set_index("Time", drop=True, inplace=True)  # Time is stored in UTC
        return df

    def _download_prev_weather(self, url: str) -> pd.DataFrame | None:
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
        self._curs.execute("SELECT StationNumber FROM OMSZ_meta")
        self._logger.debug("Queried all stations from OMSZ_meta for filtering")

        stations = self._curs.fetchall()
        stations = [s[0] for s in stations]  # remove them from tuples

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
    def _write_prev_weather(self, df: pd.DataFrame) -> None:
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

        self._df_to_sql(df, "OMSZ_data")

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
        self._curs.execute(f"SELECT * FROM OMSZ_status "
                           f"WHERE StationNumber = {station} AND "
                           f"(StartDate IS NULL OR StartDate > \"{last_year}-12-31 23:50:00\") "
                           )

        self._logger.debug(f"Queried station {station} from OMSZ_status to check is historical data is needed")

        return bool(self._curs.fetchone())

    def update_hist_weather(self) -> None:
        """
        Update historical weather data
        :returns: None
        """
        self._logger.info("Downloading and updating with historical weather data")

        hist_urls = self._get_weather_downloads(
            "https://odp.met.hu/climate/observations_hungary/10_minutes/historical/")
        for url in hist_urls:
            if self._is_hist_needed(url):
                data = self._download_prev_weather(url)
                if data is None:
                    continue
                self._write_prev_weather(data)
            else:
                self._logger.debug(f"Historical data not needed at {url}")

        self._logger.info("Finished downloading and updating with historical weather data")

    def update_rec_weather(self) -> None:
        """
        Update recent weather data
        :returns: None
        """
        self._logger.info("Downloading and updating with recent weather data")

        # Rec is always read, since it's in the between hist and curr, it's harder to validate if it's needed
        rec_urls = self._get_weather_downloads("https://odp.met.hu/climate/observations_hungary/10_minutes/recent/")
        for url in rec_urls:
            data = self._download_prev_weather(url)
            if data is None or data.empty or len(tuple(data.columns)) == 0:
                continue
            self._write_prev_weather(data)

        self._logger.info("Finished downloading and updating with recent weather data")

    def _format_past24h_weather(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.drop(columns=[col for col in df if col not in self._RENAME.keys()], inplace=True)
        df.rename(columns=self._RENAME, inplace=True)
        # Time is in UTC
        df.set_index("Time", drop=True, inplace=True)
        return df

    def _download_curr_weather(self, url: str) -> pd.DataFrame | None:
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
    def _write_curr_weather(self, df: pd.DataFrame) -> None:
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

        self._df_to_sql(df, "OMSZ_data")

    @DatabaseConnect._db_transaction
    def update_past24h_weather(self) -> None:
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
            data = self._download_curr_weather(url)
            self._logger.debug(f"Past 24h of weather data recieved from '{url}'")
            if data is None:
                continue
            self._write_curr_weather(data)

        self._logger.info("Finished downloading and updating with the past 24h of weather data")

    @DatabaseConnect._db_transaction
    def update_curr_weather(self) -> None:
        """
        Updates weather data with the current LATEST entries
        Useful if data is being requested every time there are new entries
        :returns: None
        """
        # WHILE UPDATING, THIS FUNCTION DOES A SINGLE TRANSACTION,
        # THIS IS BECAUSE FUNCTIONS USED EXPECT AN ONGOING TRANSACTION
        # see also: self.update_past24h_weather_data()
        self._logger.info("Downloading and updating with the most recent weather data")

        data = self._download_curr_weather(
            "https://odp.met.hu/weather/weather_reports/synoptic/hungary/10_minutes/csv/HABP_10M_SYNOP_LATEST.csv.zip")
        self._logger.debug("Most recent weather data recieved")
        self._write_curr_weather(data)

        self._logger.info("Finished downloading and updating with the most recent weather data")

    @DatabaseConnect._db_transaction
    def _get_max_end_date(self) -> pd.Timestamp | None:
        """
        Gets the maximum of EndDate in omsz_meta
        :returns: pandas.Timestamp for max end date
        """
        self._curs.execute("SELECT MAX(EndDate) FROM OMSZ_status")
        date = self._curs.fetchone()[0]

        self._logger.debug("Queried maximum EndDate from OMSZ_status")
        return pd.to_datetime(date, format="%Y-%m-%d %H:%M:%S")

    def choose_curr_update(self) -> bool:
        """
        Chooses to do a current weather data update if necessary
        This function assumes that hist/recent data are already updated
        :returns: did an update happen?
        """
        now: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
        end: pd.Timestamp = self._get_max_end_date()
        # adding 10 seconds, because there is a little delay in updates on omsz
        if now > (end + pd.DateOffset(minutes=20, seconds=10)):
            self.update_meta()
            if now < (end + pd.DateOffset(minutes=30)):
                self.update_curr_weather()
            else:
                self.update_past24h_weather()
            return True
        return False

    def startup_sequence(self) -> None:
        """
        Sets up tables, views, calls meta, historical/recent and past24h updates
        :returns: None
        """
        # Order of operations justified in comments
        self._create_tables_views()
        self.update_meta()
        # After tables are created it's time to update
        # Starting with historical since it doesn't affect any of the following ones and is a long running operation
        self.update_hist_weather()
        # Updating past24h first because data from 24h ago will be there
        self.update_past24h_weather()
        # Recent comes next (current year)
        # Doing this after the past24h ensures that there are no gaps happening at the t-24h mark
        # (Theoretically reverse order could result in missing t-24h if it passes a 10 min mark during it)
        self.update_rec_weather()
        # Recent update could result in passingMAX a 10 min mark
        self.choose_curr_update()

