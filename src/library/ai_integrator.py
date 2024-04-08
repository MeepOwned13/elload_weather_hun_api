import logging
from pathlib import Path
import pandas as pd
import numpy as np
from .utils.db_connect import DatabaseConnect
from .utils.tsm_wrapper import TSMWrapper
from .utils.wrappers import S2STSWrapper
from .utils.ai_utils import make_ai_df
from .utils.torch_model_definitions import Seq2seq

ai_integrator_logger = logging.getLogger("ai")
ai_integrator_logger.setLevel(logging.DEBUG)
ai_integrator_logger.addHandler(logging.NullHandler())


class AIIntegrator(DatabaseConnect):
    """
    Class to update AI tables with data inside given Database
    Relies on OMSZ_data and MAVIR_data tables defined by OMSZDownloader and MAVIRDownlader classes
    CALL startup_sequence() TO CREATE ALL REQUIRED TABLES
    """

    def __init__(self, db_connect_info: dict, model_dir: Path):
        """
        :param db_connect_info: connection info for MySQL connector
        """
        super().__init__(db_connect_info, ai_integrator_logger)
        self._model_dir = model_dir
        self._wrapper: TSMWrapper = None
        self._model_year: int = None
        self._from_time = pd.Timestamp("2015-01-01 0:00:00")

    def __del__(self):
        super().__del__()

    @DatabaseConnect._db_transaction
    def _create_tables_views_triggers(self):
        """
        Creates necessary data table, aggregate view and maintaning triggers
        """
        self._logger.debug("Starting to create tables, views, triggers that don't exist")

        self._curs.execute(
            f"""
            CREATE TABLE IF NOT EXISTS AI_10min(
                Time DATETIME PRIMARY KEY,
                NetSystemLoad REAL,
                Prec REAL,
                Temp REAL,
                RHum REAL,
                GRad REAL,
                Pres REAL,
                Wind REAL
                )
            SELECT * FROM (
                SELECT Time, NetSystemLoad FROM MAVIR_data
                WHERE Time >= "{self._from_time}") m
            NATURAL JOIN (
                SELECT Time, SUM(Prec) Prec, AVG(Temp) Temp, AVG(RHum) RHum,
                       AVG(GRad) GRad, AVG(Pres) Pres, AVG(AvgWS) Wind
                FROM OMSZ_data FORCE INDEX(OMSZ_data_time_index)
                WHERE Time >= "{self._from_time}" GROUP BY Time) o
            """
        )

        # This view aggregates to hourly data
        # from HOUR:01:01 to HOUR+1:00:00 -> HOUR:00:00
        # This means that the last entry is invalid if the full hour hasn't passed yet (filtered in the HAVING clause)
        self._curs.execute(
            """
            CREATE OR REPLACE VIEW AI_1hour AS SELECT
               FROM_UNIXTIME(CEIL(UNIX_TIMESTAMP(Time) / 3600) * 3600) Time,
                  AVG(NetSystemLoad) NetSystemLoad,
                  SUM(Prec) Prec,
                  AVG(Temp) Temp,
                  AVG(RHum) RHum,
                  AVG(GRad) GRad,
                  AVG(Pres) Pres,
                  AVG(Wind) Wind
               FROM AI_10min
               GROUP BY FROM_UNIXTIME(CEIL(UNIX_TIMESTAMP(Time) / 3600) * 3600)
               HAVING FROM_UNIXTIME(CEIL(UNIX_TIMESTAMP(Time) / 3600) * 3600) <=
                      (SELECT Time FROM AI_10min WHERE Temp IS NOT NULL ORDER BY Time DESC LIMIT 1);
            """
        )

        # AI_10min's consistency is guaranteed by the following triggers on MAVIR_data and OMSZ_data
        mavir_ins_upd =\
            f"""
            BEGIN
               IF NEW.Time >= "{self._from_time}" AND NEW.NetSystemLoad IS NOT NULL THEN
                  INSERT INTO AI_10min(Time, NetSystemLoad) VALUES (NEW.Time, NEW.NetSystemLoad)
                  ON DUPLICATE KEY UPDATE NetSystemLoad=NEW.NetSystemLoad;
               END IF;
            END;
            """

        self._curs.execute(
            f"CREATE TRIGGER IF NOT EXISTS ai_mav_ai AFTER INSERT ON MAVIR_data FOR EACH ROW {mavir_ins_upd}")
        self._curs.execute(
            f"CREATE TRIGGER IF NOT EXISTS au_mav_ai AFTER UPDATE ON MAVIR_data FOR EACH ROW {mavir_ins_upd}")
        self._curs.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS ad_mav_ai
            AFTER DELETE
            ON MAVIR_data FOR EACH ROW
            BEGIN
               IF OLD.Time >= "{self._from_time}" AND OLD.NetSystemLoad IS NOT NULL THEN
                  UPDATE AI_10min SET NetSystemLoad=NULL WHERE Time=OLD.Time;
               END IF;
            END;
            """
        )

        omsz_ins_upd_del =\
            f"""
            BEGIN
               DECLARE Prec_ REAL;
               DECLARE Temp_ REAL;
               DECLARE RHum_ REAL;
               DECLARE GRad_ REAL;
               DECLARE Pres_ REAL;
               DECLARE Wind_ REAL;

               IF NEW.Time >= "{self._from_time}" THEN
                  SELECT SUM(Prec) Prec, AVG(Temp) Temp, AVG(RHum) RHum,
                     AVG(GRad) GRad, AVG(Pres) Pres, AVG(AvgWS) Wind
                  INTO Prec_, Temp_, RHum_, GRad_, Pres_, Wind_
                  FROM OMSZ_data
                  WHERE Time = NEW.Time GROUP BY Time;

                  INSERT INTO AI_10min(Time, Prec, Temp, RHum, GRad, Pres, Wind)
                  VALUES (NEW.Time, Prec_, Temp_, RHum_, GRad_, Pres_, Wind_)
                  ON DUPLICATE KEY UPDATE Prec=Prec_, Temp=Temp_, RHum=RHum_, Pres=Pres_, GRad=GRad_, Wind=Wind_;
               END IF;
            END;
            """

        self._curs.execute(
            f"CREATE TRIGGER IF NOT EXISTS ai_omsz_ai AFTER INSERT ON OMSZ_data FOR EACH ROW {omsz_ins_upd_del}")
        self._curs.execute(
            f"CREATE TRIGGER IF NOT EXISTS au_omsz_ai AFTER UPDATE ON OMSZ_data FOR EACH ROW {omsz_ins_upd_del}")
        self._curs.execute(
            f"CREATE TRIGGER IF NOT EXISTS ad_omsz_ai AFTER DELETE ON OMSZ_data FOR EACH ROW "
            f"{omsz_ins_upd_del.replace('NEW', 'OLD')}")

        self._curs.execute(
            """
            CREATE TABLE IF NOT EXISTS S2S_raw_preds(
                Time DATETIME PRIMARY KEY,
                NSLTplus1 REAL,
                NSLTplus2 REAL,
                NSLTplus3 REAL
                )
            """
        )

        self._curs.execute(
            """
            CREATE OR REPLACE VIEW S2S_status as
            SELECT MIN(Time) StartDate, MAX(Time) EndDate FROM S2S_raw_preds;
            """
        )

        # Adding 3 extra rows so we can see all the predictions even at the end
        # where for example 1ago doesn't exits but 2ago does and so on...
        self._curs.execute(
            """
            CREATE OR REPLACE VIEW S2S_aligned_preds AS
            SELECT Time,
                   LAG(NSLTplus1, 1, NULL) OVER(ORDER BY Time) NSLP1ago,
                   LAG(NSLTplus2, 2, NULL) OVER(ORDER BY Time) NSLP2ago,
                   LAG(NSLTplus3, 3, NULL) OVER(ORDER BY Time) NSLP3ago
            FROM (
               SELECT Time, NSLTplus1, NSLTplus2, NSLTplus3 FROM S2S_raw_preds
               UNION ALL
               SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(Time)) + 3600 * 1) Time, NULL, NULL, NULL FROM S2S_raw_preds
               UNION ALL
               SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(Time)) + 3600 * 2) Time, NULL, NULL, NULL FROM S2S_raw_preds
               UNION ALL
               SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(MAX(Time)) + 3600 * 3) Time, NULL, NULL, NULL FROM S2S_raw_preds
            ) extend
            ORDER BY Time;
            """
        )

        self._logger.debug("Created tables, views, triggers that didn't exist")

    def _load_model(self, year: int):
        """
        Load model for given year prediction into self._wrapper
        :param year: year the model should predict
        """
        if self._model_year == year:
            return
        paths = list(self._model_dir.glob(f"*seq2seq_{year}.pth*"))
        if len(paths) != 1:
            # This is here to allow a full training cycle if a new year rolls around
            paths = list(self._model_dir.glob(f"*seq2seq_{year-1}.pth*"))
            if len(paths) != 1:
                raise LookupError(f"Directory \"{self._model_dir}\" should include 1 file matching "
                                  f"seq2seq_{year}.pth or seq2seq_{year-1}.pth")
            year -= 1
            self._logger.warning(f"Seq2Seq {year} model not found, falling back to Seq2Seq {year-1} model")

        self._wrapper = S2STSWrapper(Seq2seq(11, 3, 10, 1, True, 0.5, 0.05), 24, 3)
        self._wrapper.load_state(paths[0])
        self._model_year = year

    @DatabaseConnect._assert_transaction
    def _get_ai1hour_df(self, start: pd.Timestamp | None = None) -> pd.DataFrame:
        """
        Query AI_1hour table and make ai ready pandas DataFrame
        :param start: optional start_time (inclusive)
        :returns: ai ready pd.DataFrame
        """
        if start:
            # Need to request 24 before, to have lag feature after make_ai_df
            df: pd.DataFrame = pd.read_sql(f"SELECT Time, NetSystemLoad, Prec, GRad FROM AI_1hour "
                                           f"WHERE Time >= \"{start - pd.DateOffset(hours=24)}\" ORDER BY Time ASC",
                                           con=self._con)
        else:
            df: pd.DataFrame = pd.read_sql("SELECT Time, NetSystemLoad, Prec, GRad FROM AI_1hour ORDER BY Time ASC",
                                           con=self._con)
        df.set_index("Time", inplace=True, drop=True)
        return make_ai_df(df)[start:]

    def _predict_with_model(self, df: pd.DataFrame, year: int):
        """
        Predict from pandas.DataFrame with given model
        :param df: DataFrame with data ready for model
        :param year: year of model to use
        :returns: array with predictions, use caution when assigning time to them (seq_len dependant)
        """
        # Need to adjust for shapes because TimeSeriesDataset will be created in model
        x = df.to_numpy(dtype=np.float32)
        x = np.vstack((x, np.zeros((4, x.shape[1]), dtype=np.float32)))
        # we don't care about Y, but predict needs it for shapes
        self._load_model(year)
        preds, _ = self._wrapper.predict(x, np.zeros((x.shape[0])))
        return preds

    @DatabaseConnect._assert_transaction
    def _write_preds(self, preds: np.array, index: pd.Index):
        """
        Write predictions to table, uses given index for Time
        :param preds: numpy array of predictions
        :param index: pandas index Time to use for insertion
        :returns: None
        """
        pred_df = pd.DataFrame(preds, columns=["NSLTplus1", "NSLTplus2", "NSLTplus3"])
        pred_df["Time"] = index
        pred_df.set_index("Time", drop=True, inplace=True)
        self._df_to_sql(pred_df, "S2S_raw_preds")

    @DatabaseConnect._assert_transaction
    def _update_years(self, years: list[int]):
        """
        Updates S2S_raw_preds on given years
        :param years: list of years to consider, should be valid for AI_1hour
        :returns: None
        """
        df_start = pd.Timestamp(year=min(years), month=1, day=1, hour=0) - pd.DateOffset(hours=23)
        df = self._get_ai1hour_df(df_start)
        for year in years:
            start = pd.Timestamp(year=year, month=1, day=1, hour=0)
            end = pd.Timestamp(year=year, month=12, day=31, hour=23)
            subset = df[start - pd.DateOffset(hours=23):end]

            preds = self._predict_with_model(subset, year)
            self._write_preds(preds, subset[start:].index)
            self._logger.info(f"Updated s2s raw preds for year {year}")

    @DatabaseConnect._assert_transaction
    def _update_curr_year(self, start: pd.Timestamp):
        """
        Updates S2S_raw_preds starting with start
        :param start: time to start at (inclusive)
        :returns: None
        """
        df = self._get_ai1hour_df(start - pd.DateOffset(hours=23))
        preds = self._predict_with_model(df, start.year)
        self._write_preds(preds, df[start:].index)
        self._logger.info(f"Updated s2s raw preds for current year starting at {start}")

    @DatabaseConnect._db_transaction
    def choose_update(self) -> bool:
        """
        Chooses how to update S2S_raw_preds and performs it
        :returns: did an update happen?
        """
        # Checking for the start of the data
        self._curs.execute("SELECT MIN(Time), MAX(Time) FROM S2S_raw_preds")
        start, end = self._curs.fetchall()[0]
        start, end = pd.Timestamp(start), pd.Timestamp(end)
        pred_from = self._from_time + pd.DateOffset(years=2)
        # from_time.year + 1 since we don't train models with less than 1 year of data
        years_to_update = [i for i in range(pred_from.year, pd.Timestamp.now().year + 1)]
        if start == pred_from:
            # adding 1 hour to end here, if the data ends on year.12.31 23:00:00, that year don't needs predicting
            end_plus_1h = end + pd.DateOffset(hours=1)
            years_to_update = [i for i in years_to_update if i >= end_plus_1h.year]

        if len(years_to_update) > 1:
            self._update_years(years_to_update)
            return True  # if this update happened current year is already done
        # At least 1 year always remains, the current year

        # This check should be enough, checking 10min since it is stored on disk -> faster
        self._curs.execute("SELECT MAX(Time) FROM AI_10min WHERE NetSystemLoad IS NOT NULL AND Temp IS NOT NULL")
        available = pd.Timestamp(self._curs.fetchone()[0])
        if end.floor(freq='h') != available.floor(freq='h'):
            self._update_curr_year(end + pd.DateOffset(hours=1))
            return True
        # No update happens if everything was up to date
        return False

    def startup_sequence(self):
        """
        Sets up tables, views and triggers
        :returns: None
        """
        self._create_tables_views_triggers()
        self.choose_update()

