import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from .utils.db_connect import DatabaseConnect
# sqlite3 implicitly imported via DatabaseConnect


reader_logger = logging.getLogger("reader")
reader_logger.setLevel(logging.DEBUG)
reader_logger.addHandler(logging.NullHandler())


class Reader(DatabaseConnect):
    def __init__(self, db_path: Path):
        super().__init__(db_path, reader_logger)
        self._SINGLE_TABLE_LIMIT = (3 * 365 + 2 * 366) * 24 * 60  # at least 5 years
        self._WEATHER_ALL_STATIONS_LIMIT = 7 * 24 * 60  # 1 week

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

    def _limit_timeframe(self, start_date: pd.Timestamp | datetime, end_date: pd.Timestamp | datetime, minutes: int):
        """
        Check if end_date - start_date is over the limit
        :param start_date: Start date in UTC
        :param end_date: End date in UTC
        :returns: None
        :raises ValueError: if the timeframe is over the limit
        """
        if end_date - start_date > pd.Timedelta(minutes=minutes):
            raise ValueError(f"Given timeframe is over the {minutes} minute limit")

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

        table_cols = self._curs.execute(f"SELECT name FROM PRAGMA_TABLE_INFO(\"{table}\")").fetchall()
        table_cols = {tc[0].lower(): tc[0] for tc in table_cols}
        cols = [c.lower() for c in cols]

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
    def get_electricity_meta(self) -> pd.DataFrame:
        self._logger.info("Reading MAVIR_meta")
        df = pd.read_sql("SELECT * FROM MAVIR_meta", con=self._con)
        df.set_index("Column", drop=True, inplace=True)
        return df

    @DatabaseConnect._db_transaction
    def get_electricity_columns(self) -> list[str]:
        """
        Retrieves columns for MAVIR_electricity
        :returns: list of columns
        """
        table_cols = self._curs.execute("SELECT name FROM PRAGMA_TABLE_INFO(\"MAVIR_electricity\")").fetchall()
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

        columns = self._cols_to_str(self._get_valid_cols("MAVIR_electricity", cols))

        self._logger.info(f"Reading MAVIR_electricity from {start_date} to {end_date}")

        df = pd.read_sql(f"SELECT {columns} FROM MAVIR_electricity "
                         f"WHERE Time BETWEEN datetime(\"{start_date}\") AND datetime(\"{end_date}\")",
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
    def get_weather_station_columns(self, station: int) -> list[str]:
        """
        Retrieves columns for given station
        :param station: Which station to choose
        :returns: list of columns
        :raises ValueError: if param types
        :raises LookupError: if station doesn't exist
        """
        self._check_int(station, "station")

        exists = self._curs.execute(f"SELECT StationNumber FROM OMSZ_meta WHERE StationNumber = {station}").fetchone()
        if not exists:
            raise LookupError(f"Can't find station {station}")

        table_cols = self._curs.execute(f"SELECT name FROM PRAGMA_TABLE_INFO(\"OMSZ_{station}\")").fetchall()
        return [tc[0] for tc in table_cols]

    @DatabaseConnect._db_transaction
    def get_weather_all_columns(self) -> dict:
        """
        Retrieves columns for all stations
        :returns: dict{station: [columns]}
        """

        stations = self._curs.execute("SELECT StationNumber FROM OMSZ_meta").fetchall()
        result = dict()
        for station in [s[0] for s in stations]:
            table_cols = self._curs.execute(f"SELECT name FROM PRAGMA_TABLE_INFO(\"OMSZ_{station}\")").fetchall()
            result[station] = [tc[0] for tc in table_cols]

        return result

    @DatabaseConnect._db_transaction
    def get_weather_station(self, station: int, start_date: pd.Timestamp | datetime,
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

        exists = self._curs.execute(f"SELECT StationNumber FROM OMSZ_meta WHERE StationNumber = {station}").fetchone()
        if not exists:
            raise LookupError(f"Can't find station {station}")

        columns = self._cols_to_str(self._get_valid_cols(f"OMSZ_{station}", cols))

        self._logger.info(f"Reading OMSZ_{station} from {start_date} to {end_date}")

        df = pd.read_sql(f"SELECT {columns} FROM OMSZ_{station} "
                         f"WHERE Time BETWEEN datetime(\"{start_date}\") AND datetime(\"{end_date}\")",
                         con=self._con)
        df.set_index("Time", drop=True, inplace=True)

        return df

    @DatabaseConnect._db_transaction
    def get_weather_time(self, start_date: pd.Timestamp | datetime, end_date: pd.Timestamp | datetime,
                         cols: list[str] | None, df_to_dict: dict | None = None,
                         stations: list[int] | None = None) -> dict:
        """
        Get weather for all stations in given timeframe
        :param start_date: Date to start at in UTC
        :param end_date: Date to end on in UTC
        :param cols: Which columns to SELECT, None or [] means all
        :param stations: Which stations to SELECT, None or [] means all
        :returns: dict{StationNumber: {Time: data}}, skips entries where none of the specified columns exist
        :raises ValueError: if param types or timeframe length wrong
        """
        self._check_date(start_date, "start_date")
        self._check_date(end_date, "end_date")

        if not stations:
            stations = []
        for i, station in enumerate(stations):
            self._check_int(station, f"Station{i}")

        self._limit_timeframe(start_date, end_date, self._WEATHER_ALL_STATIONS_LIMIT)

        df_to_dict = df_to_dict or {"orient": "index"}

        station_df = pd.read_sql("SELECT StationNumber, StartDate, EndDate FROM OMSZ_meta",
                                 con=self._con, parse_dates=["StartDate", "EndDate"])

        self._logger.info(f"Reading {'all' if not stations else len(stations)}"
                          f"stations from {start_date} to {end_date}")

        result = dict()
        for _, row in station_df.iterrows():
            if stations and row["StationNumber"] not in stations:
                continue
            if row["StartDate"] is not pd.NaT and row["EndDate"] is not pd.NaT:
                try:
                    columns = self._cols_to_str(self._get_valid_cols(f"OMSZ_{row['StationNumber']}", cols))
                except LookupError:
                    continue

                df = pd.read_sql(f"SELECT {columns} FROM OMSZ_{row['StationNumber']} "
                                 f"WHERE Time BETWEEN datetime(\"{start_date}\") AND datetime(\"{end_date}\")",
                                 con=self._con)
                df.set_index("Time", drop=True, inplace=True)
                result[row["StationNumber"]] = df.replace({np.nan: None}).to_dict(**df_to_dict)

        self._logger.info("DONE")

        return result

