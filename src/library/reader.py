import logging
import pandas as pd
from datetime import datetime
from .utils.db_connect import DatabaseConnect
from copy import copy


reader_logger = logging.getLogger("reader")
reader_logger.setLevel(logging.DEBUG)
reader_logger.addHandler(logging.NullHandler())


class CacheEntry:
    def __init__(self, df: pd.DataFrame, min_date: pd.Timestamp | datetime | None):
        """
        Init cache entry, empty min_date means the entire underlying table is cached
        :param df: pandas DataFrame to cache, gets copied
        :param min_date: start date for cache, if None then it's a full cache of the underlying table
        """
        self._df: pd.DataFrame = df.copy(deep=True)
        self._min_date: pd.Timestamp | datetime | None = copy(min_date)

    @property
    def df(self) -> pd.DataFrame:
        return self._df.copy()

    @property
    def min_date(self) -> pd.Timestamp | datetime | None:
        return copy(self._min_date)


class Cache:
    def __init__(self):
        self._entries = {}

    def set_entry(self, name: str, df: pd.DataFrame, min_date: datetime | pd.Timestamp | None = None):
        """
        Add or replace entry in cache
        :param name: name of entry
        :param df: pandas DataFrame to cache, will be copied
        :param min_date: start date for cache entry, if None then it's a full cache of the underlying table
        """
        self._entries[name] = CacheEntry(df, min_date)

    def get_entry(self, name: str) -> CacheEntry:
        """
        Returns none if entry is not found
        :param name: name of entry to get
        """
        return self._entries.get(name, None)

    def __getitem__(self, key: str) -> CacheEntry:
        return self.get_entry(key)

    def invalidate_entry(self, name: str) -> None:
        """
        Remove entry because data might have changed, no operation if entry is already removed
        :param name: entry to remove
        """
        try:
            del self._entries[name]
        except KeyError:
            pass  # it's removed already


class Reader(DatabaseConnect):
    """
    Facilitates reading of the Database, returning results ready for the API
    """

    def __init__(self, db_connect_info: dict):
        """
        :param db_connect_info: connection info for MySQL connector
        """
        super().__init__(db_connect_info, reader_logger)
        self._SINGLE_TABLE_LIMIT: pd.Timedelta = pd.Timedelta(weeks=52 * 4 + 1)  # 4 years
        self._WEATHER_ALL_STATIONS_LIMIT: pd.Timedelta = pd.Timedelta(days=7)
        self._cache: Cache = Cache()
        self._TIME_FORMAT: str = "%Y-%m-%d %H:%M:%S"

    def __del__(self):
        super().__del__()

    def _check_int(self, integer, name: str):
        """
        Check if arg is integer
        :param integer: check this
        :param name: name to write into error message
        :returns: None
        :raises TypeError: if arg is not an int
        """
        if type(integer) is not int:
            raise TypeError(f"Param {name} is not of type int")

    def _check_date(self, date, name: str):
        """
        Check if arg is pandas.Timestamp or datetime.datetime
        :param date: check this
        :param name: name to write into error message
        :returns: None
        :raises TypeError: if arg is not a pandas.Timestamp or datetime.datetime
        """
        if type(date) is not pd.Timestamp and type(date) is not datetime:
            raise ValueError(f"Param {name} is not of type pandas.Timestamp or datetime.datetime")

    def _limit_timeframe(self, start_date: pd.Timestamp | datetime, end_date: pd.Timestamp | datetime,
                         limit: pd.Timedelta):
        """
        Check if end_date - start_date is over the limit
        :param start_date: Start date in UTC
        :param end_date: End date in UTC
        :returns: None
        :raises ValueError: if the timeframe is over the limit
        """
        if end_date - start_date > limit:
            raise ValueError(f"Given timeframe is over the limit of {limit}")

    def _get_valid_cols(self, table: str, cols: list[str] | None) -> list[str] | None:
        """
        Retrieve valid columns for given table, case insensitive
        THIS FUNCTION ASSUMES THERE IS AN ONGOING TRANSACTION
        :param table: Table to check for columns
        :param cols: List of columns, None -> returns None
        :returns: List of valid columns in correct Case present in table, None if cols was None or []
        :raises LookupError: if no columns are valid
        """
        if not cols:
            return None

        self._curs.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}'")
        table_cols = self._curs.fetchall()
        table_cols = {tc[0].lower(): tc[0] for tc in table_cols}
        cols = list(set([c.lower() for c in cols]))

        valid = []
        for col in cols:
            if col in table_cols:
                valid.append(table_cols[col])

        if len(valid) == 0:
            raise LookupError("No valid columns")

        return valid

    def _cols_to_str(self, cols: list[str] | None) -> str:
        """
        Transform cols to SQL string, BEWARE TO CHECK VALIDITY OF COLUMNS FIRST!
        :param cols: list of columns
        :returns: * if cols is None, else valid str for SELECT ... FROM with Time added if it's not already there!
        """
        if cols:
            if "Time" in cols:
                return str(cols)[1:-1].replace('\'', "")
            return ("Time, " + str(cols)[1:-1]).replace('\'', "")
        else:
            return '*'

    @DatabaseConnect._db_transaction
    def refresh_caches(self, sections: list[str] | str) -> None:
        """
        Refresh cache for given sections, partial cache gets refilled, others just removed
        :param sections: single str or list of sections to refresh cache for ("mavir", "omsz", "ai", "s2s")
        """
        # Allow a single str
        if type(sections) is str:
            sections = [sections]

        now = pd.Timestamp.now("UTC").tz_localize(None)

        # The idea for caching
        # - pre-cache important views and larger tables <- high likelihood of them being requested, and not in full
        # - on-demand-cache views and tables which are already fast to request and are always requested in full

        # MAVIR cache
        if "mavir" in sections:
            self._logger.debug("Refreshing MAVIR cache")
            self._cache.invalidate_entry("MAVIR_status")  # on-demand, done inside get_electricity_status

            from_date = now - pd.DateOffset(days=30)
            df = pd.read_sql(
                f"SELECT * FROM MAVIR_data WHERE Time > \"{from_date.strftime(self._TIME_FORMAT)}\"", con=self._con)
            df.set_index("Time", inplace=True, drop=True)
            self._cache.set_entry("MAVIR_data", df, from_date)  # pre-cache

            self._logger.info("Refreshed MAVIR cache")

        # OMSZ cache
        if "omsz" in sections:
            self._logger.debug("Refreshing OMSZ cache")
            self._cache.invalidate_entry("OMSZ_meta")  # on-demand, done inside get_weather_meta
            self._cache.invalidate_entry("OMSZ_status")  # on-demand, done inside get_weather_status

            from_date = now - pd.DateOffset(days=14)
            df = pd.read_sql(
                f"""SELECT * FROM OMSZ_data FORCE INDEX(OMSZ_data_time_index)
                    WHERE Time > \"{from_date.strftime(self._TIME_FORMAT)}\"""",
                self._con)
            df.set_index("Time", inplace=True, drop=True)
            self._cache.set_entry("OMSZ_data", df, from_date)  # pre-cache

            self._logger.info("Refreshed OMSZ cache")

        # AI table cache
        if "ai" in sections:
            self._logger.debug("Refreshing AI cache")

            df = pd.read_sql("SELECT * FROM AI_10min", con=self._con)
            df.set_index("Time", drop=True, inplace=True)
            self._cache.set_entry("AI_10min", df)  # pre-cache, full because requests allow to request table in full

            df = pd.read_sql("SELECT * FROM AI_1hour", con=self._con)
            df.set_index("Time", drop=True, inplace=True)
            self._cache.set_entry("AI_1hour", df)  # pre-cache, because harder to calculate view

            self._logger.info("Refreshed AI cache")

        # S2S preds cache
        if "s2s" in sections:
            self._logger.debug("Refreshing S2S cache")
            self._cache.invalidate_entry("S2S_status")  # on-demand, done inside get_s2s_status

            df = pd.read_sql("SELECT * FROM S2S_raw_preds s2s", con=self._con)
            df.set_index("Time", drop=True, inplace=True)
            self._cache.set_entry("S2S_raw_preds", df)  # pre-cache, full because requests allow to request view in full

            df = pd.read_sql(
                """SELECT s2s.Time, tr.NetSystemLoad, s2s.NSLP1ago, s2s.NSLP2ago, s2s.NSLP3ago
                   FROM (SELECT Time, NetSystemLoad FROM AI_1hour) tr RIGHT JOIN S2S_aligned_preds s2s
                   ON tr.Time = s2s.Time""",
                con=self._con)
            df.set_index("Time", drop=True, inplace=True)
            self._cache.set_entry("S2S_aligned_preds", df)  # pre-cache, because harder to calculate view

            self._logger.info("Refreshed S2S cache")

    @DatabaseConnect._db_transaction
    def get_electricity_status(self) -> pd.DataFrame:
        cached = self._cache["MAVIR_status"]
        if cached:
            self._logger.info("CACHED Reading MAVIR_status")
            df = cached.df
        else:
            self._logger.info("Reading MAVIR_status")
            df = pd.read_sql("SELECT * FROM MAVIR_status", con=self._con)
            df.set_index("Column", drop=True, inplace=True)
            self._cache.set_entry("MAVIR_status", df)
        return df

    @DatabaseConnect._db_transaction
    def get_electricity_columns(self) -> list[str]:
        """
        Retrieves columns for MAVIR_data: returns: list of columns
        """
        self._logger.info("Reading columns of MAVIR_data")
        self._curs.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='MAVIR_data'")
        table_cols = self._curs.fetchall()
        return [tc[0] for tc in table_cols]

    @DatabaseConnect._db_transaction
    def get_electricity_load(self, start_date: pd.Timestamp | datetime,
                             end_date: pd.Timestamp | datetime, cols: list[str] | None) -> pd.DataFrame:
        """
        Get electricity load for given timeframe
        :param start_date: Date to start at in UTC
        :param end_date: Date to end on in UTC
        :param cols: Which columns to SELECT
        :returns: pandas.DataFrame with the data
        :raises ValueError: if param types or timeframe length wrong
        :raises LookupError: if cols don't include any valid columns
        """
        self._check_date(start_date, "start_date")
        self._check_date(end_date, "end_date")

        self._limit_timeframe(start_date, end_date, self._SINGLE_TABLE_LIMIT)

        columns = self._get_valid_cols("MAVIR_data", cols)

        cached = self._cache["MAVIR_data"]

        if cached and cached.min_date < start_date:
            self._logger.info(f"CACHED Reading MAVIR_data from {start_date} to {end_date}")
            df = cached.df[start_date:end_date][columns] if columns else cached.df[start_date:end_date]
        else:
            self._logger.info(f"Reading MAVIR_data from {start_date} to {end_date}")
            df = pd.read_sql(
                f"""SELECT {self._cols_to_str(columns)} FROM MAVIR_data
                WHERE Time BETWEEN \"{start_date.strftime(self._TIME_FORMAT)}\"
                AND \"{end_date.strftime(self._TIME_FORMAT)}\"""",
                con=self._con)
            df.set_index("Time", drop=True, inplace=True)

        return df

    @DatabaseConnect._db_transaction
    def get_weather_meta(self) -> pd.DataFrame:
        cached = self._cache["OMSZ_meta"]
        if cached:
            self._logger.info("CACHED Reading OMSZ_meta")
            df = cached.df
        else:
            self._logger.info("Reading OMSZ_meta")
            df = pd.read_sql("SELECT * FROM OMSZ_meta", con=self._con)
            df.set_index("StationNumber", drop=True, inplace=True)
            self._cache.set_entry("OMSZ_meta", df)
        return df

    @DatabaseConnect._db_transaction
    def get_weather_status(self) -> pd.DataFrame:
        cached = self._cache["OMSZ_status"]
        if cached:
            self._logger.info("CACHED Reading OMSZ_status")
            df = cached.df
        else:
            self._logger.info("Reading OMSZ_status")
            df = pd.read_sql("SELECT * FROM OMSZ_status", con=self._con)
            df.set_index("StationNumber", drop=True, inplace=True)
            self._cache.set_entry("OMSZ_status", df)
        return df

    @DatabaseConnect._db_transaction
    def get_weather_columns(self) -> list[str]:
        """
        Retrieves columns for given station
        :returns: list of columns
        """
        self._logger.info("Reading columns of OMSZ_data")
        self._curs.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='OMSZ_data'")
        table_cols = self._curs.fetchall()
        return [tc[0] for tc in table_cols]

    @DatabaseConnect._db_transaction
    def get_weather_stations(self, start_date: pd.Timestamp | datetime, end_date: pd.Timestamp | datetime,
                             cols: list[str] | None, stations: list[int] | None = None) -> pd.DataFrame:
        """
        Get weather for all stations in given timeframe
        :param start_date: Date to start at in UTC
        :param end_date: Date to end on in UTC
        :param cols: Which columns to SELECT, None or [] means all
        :param stations: Which stations to SELECT, None or [] means all
        :returns: DataFrame of retrieved data
        :raises ValueError: if param types, timeframe length wrong or incorrect station is specified
        """
        self._check_date(start_date, "start_date")
        self._check_date(end_date, "end_date")

        self._limit_timeframe(start_date, end_date, self._WEATHER_ALL_STATIONS_LIMIT)

        self._curs.execute("SELECT StationNumber FROM OMSZ_meta")
        valid_stations = set([s[0] for s in self._curs.fetchall()])

        if not stations:
            stations = []
        for i, station in enumerate(stations):
            self._check_int(station, f"Station{i}")
            if station not in valid_stations:
                raise ValueError(f"Station {station} is invalid")

        columns = self._get_valid_cols("OMSZ_data", cols)
        if columns and "StationNumber" not in columns:
            columns = ["StationNumber"] + columns

        cached = self._cache["OMSZ_data"]

        if cached and cached.min_date < start_date:
            self._logger.info(f"Reading CACHED {'all' if not stations else len(stations)} "
                              f"stations from {start_date} to {end_date}")
            df = cached.df[start_date:end_date][columns] if columns else cached.df[start_date:end_date]
            df.reset_index(inplace=True, drop=False)
            if stations:
                df = df[df["StationNumber"].isin(stations)]
        else:
            self._logger.info(f"Reading {'all' if not stations else len(stations)} "
                              f"stations from {start_date} to {end_date}")
            if stations:
                # It'll use PRIMARY index (StationNumber, Time) => Very fast
                df = pd.read_sql(
                    f"""SELECT {self._cols_to_str(columns)} FROM OMSZ_data
                        WHERE StationNumber IN ({str(stations)[1:-1]}) AND
                        Time BETWEEN \"{start_date.strftime(self._TIME_FORMAT)}\" AND
                        \"{end_date.strftime(self._TIME_FORMAT)}\"""",
                    con=self._con)
            else:
                # It uses no index by default in most cases, need to force (Time) index => 3x faster
                df = pd.read_sql(
                    f"""SELECT {self._cols_to_str(columns)} FROM OMSZ_data FORCE INDEX(OMSZ_data_time_index)
                        WHERE Time BETWEEN \"{start_date.strftime(self._TIME_FORMAT)}\" AND
                        \"{end_date.strftime(self._TIME_FORMAT)}\"""",
                    con=self._con)

        return df

    @DatabaseConnect._db_transaction
    def get_ai_table_columns(self) -> list[str]:
        """
        Retrieves columns for AI_10min and AI_1hour: returns: list of columns
        """
        self._curs.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='AI_10min'")
        table_cols = self._curs.fetchall()
        return [tc[0] for tc in table_cols]

    @DatabaseConnect._db_transaction
    def get_ai_table(self, start_date: pd.Timestamp | datetime | None,
                     end_date: pd.Timestamp | datetime | None, which: str = '10min') -> pd.DataFrame:
        """
        Get electricity load for given timeframe
        :param start_date: Date to start at in UTC
        :param end_date: Date to end on in UTC
        :param which: 10min or 1hour aggregation?
        :returns: pandas.DataFrame with the data
        :raises ValueError: if param types or 'which' is wrong
        """
        if start_date:
            self._check_date(start_date, "start_date")
        if end_date:
            self._check_date(end_date, "end_date")
        if which not in ('10min', '1hour'):
            raise ValueError("'which' must be 1 of the following: '10min', '1hour'")

        cached = self._cache[f"AI_{which}"]
        if cached:
            self._logger.info(f"CACHED Reading AI_{which} from {start_date or 'start'} to {end_date or 'end'}")
            df = cached.df[start_date:end_date]
        else:
            # Theoretically, this branch doesn't see action if the cache is initialized
            self._logger.info(f"Reading AI_{which} from {start_date or 'start'} to {end_date or 'end'}")

            start_sql = f"Time >= \"{start_date.strftime(self._TIME_FORMAT)}\"" if start_date else "TRUE"
            end_sql = f"Time <= \"{end_date.strftime(self._TIME_FORMAT)}\"" if end_date else "TRUE"

            df = pd.read_sql(f"SELECT * FROM AI_{which} WHERE {start_sql} AND {end_sql}", con=self._con)
            df.set_index("Time", drop=True, inplace=True)

        return df

    @DatabaseConnect._db_transaction
    def get_s2s_status(self) -> pd.DataFrame:
        cached = self._cache["S2S_status"]
        if cached:
            self._logger.info("CACHED Reading S2S_status")
            df = cached.df
        else:
            self._logger.info("Reading S2S_status")
            df = pd.read_sql("SELECT * FROM S2S_status", con=self._con)
            df["Type"] = ["S2S"]
            df.set_index("Type", inplace=True, drop=True)
            self._cache.set_entry("S2S_status", df)
        return df

    @DatabaseConnect._db_transaction
    def get_s2s_preds(self, start_date: pd.Timestamp | datetime | None,
                      end_date: pd.Timestamp | datetime | None, aligned: bool = False) -> pd.DataFrame:
        """
        Get predictions of Seq2Seq model for given timeframe
        :param start_date: Date to start at in UTC
        :param end_date: Date to end on in UTC
        :param aligned: align true - pred or just return predictions at time
        :returns: pandas.DataFrame with the data
        :raises ValueError: if param types are wrong
        """
        if start_date:
            self._check_date(start_date, "start_date")
        if end_date:
            self._check_date(end_date, "end_date")

        start_sql = f"s2s.Time >= \"{start_date.strftime(self._TIME_FORMAT)}\"" if start_date else "TRUE"
        end_sql = f"s2s.Time <= \"{end_date.strftime(self._TIME_FORMAT)}\"" if end_date else "TRUE"

        aligned_str = "aligned" if aligned else "raw"
        cached = self._cache[f"S2S_{aligned_str}_preds"]
        if cached:
            self._logger.info(f"CACHED Reading S2S_{aligned_str}_preds and AI_1hour from {start_date or 'start'} "
                              f"to {end_date or 'end'}")
            df = cached.df[start_date:end_date]
        else:
            self._logger.info(f"Reading S2S_{aligned_str}_preds and AI_1hour from {start_date or 'start'} "
                              f"to {end_date or 'end'}")
            if aligned:
                df = pd.read_sql(
                    f"""SELECT s2s.Time, tr.NetSystemLoad, s2s.NSLP1ago, s2s.NSLP2ago, s2s.NSLP3ago
                        FROM (SELECT Time, NetSystemLoad FROM AI_1hour) tr RIGHT JOIN S2S_aligned_preds s2s
                        ON tr.Time = s2s.Time WHERE {start_sql} AND {end_sql}""",
                    con=self._con)
            else:
                df = pd.read_sql(f"SELECT * FROM S2S_raw_preds s2s WHERE {start_sql} AND {end_sql}", con=self._con)

            df.set_index("Time", drop=True, inplace=True)

        return df

