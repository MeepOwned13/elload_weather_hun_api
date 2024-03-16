from requests import Session
from pathlib import Path
import logging
import pandas as pd
import io
import re
import warnings
from .utils.db_connect import DatabaseConnect
# sqlite3 implicitly imported via DatabaseConnect

mavir_downloader_logger = logging.getLogger("mavir")
mavir_downloader_logger.setLevel(logging.DEBUG)
mavir_downloader_logger.addHandler(logging.NullHandler())


class MAVIR_Downloader(DatabaseConnect):
    def __init__(self, db_path: Path):
        super().__init__(db_path, mavir_downloader_logger)
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
            self._drop_temp()
        super().__del__()

    @DatabaseConnect._db_transaction
    def _drop_temp(self) -> None:
        self._curs_.execute("DROP TABLE IF EXISTS _temp_mavir")
        self._logger.debug("Dropped temporary tables if they existed")

    @DatabaseConnect._db_transaction
    def _create_meta(self) -> None:
        """
        Creates metadata table if it doesn't exist yet
        :returns: None
        """
        self._curs_.execute("""CREATE TABLE IF NOT EXISTS MAVIR_meta(
            Column TEXT PRIMARY KEY,
            StartDate TIMESTAMP,
            EndDate TIMESTAMP
            )""")

        self._curs_.executemany("INSERT OR IGNORE INTO MAVIR_meta (Column, StartDate, EndDate) VALUES (?, ?, ?)",
                                [(key, None, None) for key in set(self._RENAME.values()) - set(["Time"])])
        # Don't need to store StarDate and EndDate for time

    @DatabaseConnect._assert_transaction
    def _update_meta(self) -> None:
        """
        Updates metadata if MAVIR_meta and MAVIR_electricity exist
        THIS FUNCTION ASSUMES THERE IS AN ONGOING TRANSACTION
        :returns: None
        """
        exists = self._curs_.execute("SELECT name FROM sqlite_master WHERE type=\"table\" AND "
                                     "(name=\"MAVIR_electricity\" OR name=\"MAVIR_meta\")").fetchall()
        if len(exists) < 2:
            return

        records = [rec for rec in self._curs_.execute("SELECT * FROM MAVIR_meta").fetchall()]

        self._logger.info("Started metadata update")
        for col, start, end in records:
            # SELECT the minimum for the column, then update meta
            self._curs_.execute(f"UPDATE MAVIR_meta SET StartDate = ("
                                f"SELECT MIN(Time) FROM MAVIR_electricity WHERE {col} IS NOT NULL"
                                f") WHERE Column = \"{col}\"")

            # SELECT the maximum for the column, then update meta
            self._curs_.execute(f"UPDATE MAVIR_meta SET EndDate = ("
                                f"SELECT MAX(Time) FROM MAVIR_electricity WHERE {col} IS NOT NULL"
                                f") WHERE Column = \"{col}\"")

    def _format_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip()  # remove trailing whitespace
        df.rename(columns=self._RENAME, inplace=True)
        df = df[self._RENAME.values()]  # Reordering, assumes all columns exist
        # Not dropping empty columns since they will be filled later when data is available
        # Using apply here because day/night saving transition doesn't translate well to datetime types
        df["Time"] = df["Time"].apply(
            lambda o: pd.to_datetime(o, format="%Y.%m.%d %H:%M:%S %z").tz_convert("UTC").tz_localize(None))
        df.set_index("Time", drop=True, inplace=True)  # Time is stored in UTC
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
        :returns: Downloaded DataFrame
        """
        self._logger.debug(f"Requesting electricity data from {start} to {end}")
        url = (f"https://www.mavir.hu/rtdwweb/webuser/chart/7678/export"
               f"?exportType=xlsx"
               f"&fromTime={int(start.value / 1e6)}"
               f"&toTime={int(end.value / 1e6)}"
               f"&periodType=min"
               f"&period=10")

        request = self._sess.get(url)
        if request.status_code != 200:
            self._logger.error(
                f"Electricity data download failed from {start} to {end} with {request.status_code}")
            return
        self._logger.info(f"Recieved electricity data from {start} to {end}")

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
        :returns: Downloaded DataFrame
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

    @DatabaseConnect._db_transaction
    def _write_electricity_data(self, df: pd.DataFrame) -> None:
        """
        Insert electricity data, doesn't update, only inserts Times that don't exist yet
        :param df: DataFrame to use
        :returns: None
        """
        table_name = "MAVIR_electricity"
        self._logger.info("Starting write to table MAVIR_electricity")
        exists = self._curs_.execute(
            f"SELECT name FROM sqlite_master WHERE type=\"table\" AND name=\"{table_name}\"").fetchone()
        if not exists:
            self._logger.info(f"Creating new table {table_name}")
            df.to_sql(name="_temp_mavir", con=self._con, if_exists='replace')
            # I want a primary key for the table
            sql = self._curs_.execute("SELECT sql FROM sqlite_master WHERE tbl_name = \"_temp_mavir\"").fetchone()[0]
            sql = sql.replace("_temp_mavir", table_name)
            sql = sql.replace("\"Time\" TIMESTAMP", "\"Time\" TIMESTAMP PRIMARY KEY")
            self._curs_.execute(sql)
            self._curs_.execute(f"CREATE INDEX ix_{table_name}_Time ON {table_name} (Time)")
            self._logger.debug(f"Created new table {table_name}")
        else:
            # Idea: create temp table and insert values missing into the actual table
            self._logger.info(f"Table {table_name} already exists, inserting new values")
            df.to_sql(name="_temp_mavir", con=self._con, if_exists="replace")

        # Tuple->String in Python leaves a single ',' if the tuple has 1 element
        df_cols = tuple(df.columns)
        if len(df_cols) == 1:
            cols = "Time, " + str(tuple(df.columns))[1:-2].replace("\'", "")
        else:
            cols = "Time, " + str(tuple(df.columns))[1:-1].replace("\'", "")

        # SQL should look like this:
        # INSERT INTO table ([cols]) SELECT [cols] FROM temp WHERE Time NOT IN (SELECT Time FROM table)
        # Watch the first set of cols need (), but the second don't, also gonna remove ' marks
        self._curs_.execute(f"INSERT OR REPLACE INTO {table_name} ({cols}) SELECT {cols} FROM _temp_mavir ")

        self._update_meta()

        self._logger.info(
            f"Updated {table_name}, updated StartDate and EndDate in metadata for Columns")

    @DatabaseConnect._db_transaction
    def _get_min_end_date(self) -> pd.Timestamp | None:
        """
        Get MIN EndDate from MAVIR_meta, useful to know which Times need downloading
        :returns: minimum of EndDate as pd.Timestamp or None is all rows are NULL
        """
        exists = self._curs_.execute("SELECT name FROM sqlite_master WHERE type=\"table\" AND "
                                     "name=\"MAVIR_meta\"").fetchone()
        if not exists:
            return

        date = self._curs_.execute("SELECT MIN(EndDate) FROM MAVIR_meta").fetchone()[0]

        # Pandas.to_datetime becomes None if date is None
        return pd.to_datetime(date, format="%Y-%m-%d %H:%M:%S")

    def update_electricity_data(self) -> None:
        """
        Update electricity data taking metadata into account
        Updates by using the minimal EndDate from the MAVIR_meta and replaces/inserts the downloaded data
        :returns: None
        """
        self._create_meta()

        now: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
        # First available data is at 2007-01-01 00:00:00 UTC
        self._write_electricity_data(self._download_data_range(
            self._get_min_end_date() or pd.to_datetime("2007-01-01 00:00:00", format="%Y-%m-%d %H:%M:%S"),
            now.round(freq="10min") + pd.Timedelta(hours=24)))

    @DatabaseConnect._db_transaction
    def _get_end_date_netload(self) -> pd.Timestamp | None:
        """
        Gets the end date for NetSystemLoad from MAVIR_meta
        :returns: pandas.Timestamp for end date
        """
        date = self._curs_.execute("SELECT EndDate FROM MAVIR_meta WHERE Column=\"NetSystemLoad\"").fetchone()[0]
        return pd.to_datetime(date, format="%Y-%m-%d %H:%M:%S")

    def choose_update(self) -> bool:
        """
        Chooses to electricity data update if necessary, based on NetSystemLoad
        :returns: did an update happen?
        """
        end: pd.Timestamp = self._get_end_date_netload()
        now: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
        if now > (end + pd.Timedelta(minutes=10)):
            self.update_electricity_data()
            return True
        return False

