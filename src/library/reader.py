import logging
import pandas as pd
from datetime import datetime
from .utils.db_connect import DatabaseConnect
# sqlite3 implicitly imported via DatabaseConnect


reader_logger = logging.getLogger("reader")
reader_logger.setLevel(logging.DEBUG)
reader_logger.addHandler(logging.NullHandler())


class Reader(DatabaseConnect):
    """
    Facilitates reading of the Database, returning results ready for the API
    """

    def __init__(self, db_connect_info: dict):
        """
        :param db_connect_info: connection info for MySQL connector
        """
        super().__init__(db_connect_info, reader_logger)
        self._SINGLE_TABLE_LIMIT = pd.Timedelta(weeks=52 * 4 + 1)  # 4 years
        self._WEATHER_ALL_STATIONS_LIMIT = pd.Timedelta(days=7)

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
    def get_electricity_status(self) -> pd.DataFrame:
        self._logger.info("Reading MAVIR_status")
        df = pd.read_sql("SELECT * FROM MAVIR_status", con=self._con)
        df.set_index("Column", drop=True, inplace=True)
        return df

    @DatabaseConnect._db_transaction
    def get_electricity_columns(self) -> list[str]:
        """
        Retrieves columns for MAVIR_data
        :returns: list of columns
        """
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

        columns = self._cols_to_str(self._get_valid_cols("MAVIR_data", cols))

        self._logger.info(f"Reading MAVIR_data from {start_date} to {end_date}")

        df = pd.read_sql(f"SELECT {columns} FROM MAVIR_data "
                         f"WHERE Time BETWEEN \"{start_date}\" AND \"{end_date}\"",
                         con=self._con)
        df.set_index("Time", drop=True, inplace=True)

        return df

    @DatabaseConnect._db_transaction
    def get_weather_meta(self) -> pd.DataFrame:
        self._logger.info("Reading OMSZ_meta")
        df = pd.read_sql("SELECT * FROM OMSZ_meta", con=self._con)
        df.set_index("StationNumber", drop=True, inplace=True)
        return df

    @DatabaseConnect._db_transaction
    def get_weather_status(self) -> pd.DataFrame:
        self._logger.info("Reading OMSZ_status")
        df = pd.read_sql("SELECT * FROM OMSZ_status", con=self._con)
        df.set_index("StationNumber", drop=True, inplace=True)
        return df

    @DatabaseConnect._db_transaction
    def get_weather_columns(self) -> list[str]:
        """
        Retrieves columns for given station
        :returns: list of columns
        """
        self._curs.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='OMSZ_data'")
        table_cols = self._curs.fetchall()
        return [tc[0] for tc in table_cols]

    @DatabaseConnect._db_transaction
    def get_weather_one_station(self, station: int, start_date: pd.Timestamp | datetime,
                                end_date: pd.Timestamp | datetime, cols: list[str] | None) -> pd.DataFrame:
        """
        Get weather for given station in given timeframe
        :param station: Which station to read from
        :param start_date: Date to start at in UTC
        :param end_date: Date to end on in UTC
        :param cols: Which columns to SELECT
        :returns: pandas.DataFrame with the data or None
        :raises ValueError: if param types or timeframe length wrong
        :raises LookupError: if station doesn't exist, or cols don't include any valid columns
        """
        self._check_int(station, "station")
        self._check_date(start_date, "start_date")
        self._check_date(end_date, "end_date")

        self._limit_timeframe(start_date, end_date, self._SINGLE_TABLE_LIMIT)

        self._curs.execute(f"SELECT StationNumber FROM OMSZ_meta WHERE StationNumber = {station}")
        exists = self._curs.fetchone()
        if not exists:
            raise LookupError(f"Can't find station {station}")

        columns = self._cols_to_str(self._get_valid_cols("OMSZ_data", cols))

        self._logger.info(f"Reading OMSZ_{station} from {start_date} to {end_date}")

        df = pd.read_sql(f"SELECT {columns} FROM OMSZ_data WHERE StationNumber = {station} AND "
                         f"Time BETWEEN \"{start_date}\" AND \"{end_date}\"",
                         con=self._con)
        df.set_index("Time", drop=True, inplace=True)

        return df.drop(columns="StationNumber", errors='ignore')

    @DatabaseConnect._db_transaction
    def get_weather_multi_station(self, start_date: pd.Timestamp | datetime, end_date: pd.Timestamp | datetime,
                                  cols: list[str] | None, stations: list[int] | None = None) -> dict:
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
        valid_stations = self._curs.fetchall()
        valid_stations = set([s[0] for s in valid_stations])

        if not stations:
            stations = []
        for i, station in enumerate(stations):
            self._check_int(station, f"Station{i}")
            if station not in valid_stations:
                raise ValueError(f"Station {station} is invalid")

        columns = self._get_valid_cols("OMSZ_data", cols)
        if columns:
            columns = ["StationNumber"] + columns
        columns = self._cols_to_str(columns)

        self._logger.info(f"Reading {'all' if not stations else len(stations)} "
                          f"stations from {start_date} to {end_date}")

        if stations:
            # It'll use PRIMARY index (StationNumber, Time) => Very fast
            df = pd.read_sql(f"SELECT {columns} FROM OMSZ_data "
                             f"WHERE StationNumber IN ({str(stations)[1:-1]}) AND "
                             f"Time BETWEEN \"{start_date}\" AND \"{end_date}\"",
                             con=self._con)
        else:
            # It uses no index by default, need to force (Time) index => 3x faster
            df = pd.read_sql(f"SELECT {columns} FROM OMSZ_data FORCE INDEX(OMSZ_data_time_index) "
                             f"WHERE Time BETWEEN \"{start_date}\" AND \"{end_date}\"",
                             con=self._con)

        return df

    @DatabaseConnect._db_transaction
    def get_ai_table_columns(self) -> list[str]:
        """
        Retrieves columns for AI_10min and AI_1hour
        :returns: list of columns
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

        self._logger.info(f"Reading AI_{which} from {start_date or 'start'} to {end_date or 'end'}")

        start_sql = f"Time >= \"{start_date}\"" if start_date else "TRUE"
        end_sql = f"Time <= \"{end_date}\"" if end_date else "TRUE"

        df = pd.read_sql(f"SELECT * FROM AI_{which} WHERE {start_sql} AND {end_sql}",
                         con=self._con)
        df.set_index("Time", drop=True, inplace=True)

        return df

    @DatabaseConnect._db_transaction
    def get_s2s_status(self) -> pd.DataFrame:
        self._logger.info("Reading S2S_status")
        df = pd.read_sql("SELECT * FROM S2S_status", con=self._con)
        df["Type"] = ["S2S"]
        df.set_index("Type", inplace=True, drop=True)
        return df

    @DatabaseConnect._db_transaction
    def get_s2s_preds(self, start_date: pd.Timestamp | datetime | None,
                      end_date: pd.Timestamp | datetime | None, aligned=False) -> pd.DataFrame:
        """
        Get predictions of Seq2Seq model for given timeframe
        :param start_date: Date to start at in UTC
        :param end_date: Date to end on in UTC
        :param aligned: align true-pred or just return predictions at time
        :returns: pandas.DataFrame with the data
        :raises ValueError: if param types are wrong
        """
        if start_date:
            self._check_date(start_date, "start_date")
        if end_date:
            self._check_date(end_date, "end_date")

        start_sql = f"s2s.Time >= \"{start_date}\"" if start_date else "TRUE"
        end_sql = f"s2s.Time <= \"{end_date}\"" if end_date else "TRUE"

        if aligned:
            self._logger.info(f"Reading S2S_aligned_preds and AI_1hour from {start_date or 'start'} "
                              f"to {end_date or 'end'}")
            df = pd.read_sql(f"SELECT s2s.Time, tr.NetSystemLoad, s2s.NSLP1ago, s2s.NSLP2ago, s2s.NSLP3ago "
                             f"FROM (SELECT Time, NetSystemLoad FROM AI_1hour) tr RIGHT JOIN S2S_aligned_preds s2s "
                             f"ON tr.Time = s2s.Time WHERE {start_sql} AND {end_sql}", con=self._con)
        else:
            self._logger.info(f"Reading S2S_raw_preds from {start_date or 'start'} to {end_date or 'end'}")
            df = pd.read_sql(f"SELECT * FROM S2S_raw_preds s2s WHERE {start_sql} AND {end_sql}", con=self._con)

        df.set_index("Time", drop=True, inplace=True)

        return df

