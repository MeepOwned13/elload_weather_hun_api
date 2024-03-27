import logging
from pathlib import Path
import pandas as pd
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
            CREATE TABLE IF NOT EXISTS S2S_preds(
                Time DATETIME PRIMARY KEY,
                T_plus_1 REAL,
                T_plus_2 REAL,
                T_plus_3 REAL
                )
            """
        )

        self._logger.info("Created tables, views, triggers that didn't exist")

    def _load_model(self, year: int):
        """
        Load model for given year prediction into self._wrapper
        :param year: year the model should predict
        """
        paths = list(self._model_dir.glob(f"*seq2seq_{year}.pth*"))
        if len(paths) != 1:
            raise LookupError(f"Directory \"{self._model_dir}\" should include 1 file matching seq2seq_{year}.pth")
        self._wrapper = S2STSWrapper(Seq2seq(11, 3, 10, 1, True, 0.5, 0.05), 24, 3)
        self._wrapper.load_state(paths[0])

    @DatabaseConnect._db_transaction
    def _get_ai1hour_df(self) -> pd.DataFrame:
        """
        Query AI_1hour table and make ai ready pandas DataFrame
        :returns: ai ready pd.DataFrame
        """
        df: pd.DataFrame = pd.read_sql("SELECT Time, NetSystemLoad, Prec, GRad FROM AI_1hour", con=self._con)
        df.set_index("Time", inplace=True, drop=True)
        return make_ai_df(df)

    @DatabaseConnect._db_transaction
    def _update_preds(self):
        """
        Updates predictions to S2S_preds
        """
        self._curs.execute("SELECT MIN(Time), MAX(Time) FROM S2S_preds")
        start, end = self._curs.fetchall()[0]
        pred_from = self._from_time.replace(self._from_time.year + 1)
        # from_time.year + 1 since we don't train models with less than 1 year of data
        years_to_update = [i for i in range(pred_from.year, pd.Timestamp.now().year + 1)]
        if start == pred_from:
            years_to_update = [i for i in years_to_update if i >= end.year]
        print(years_to_update)

    def startup_sequence(self):
        """
        Sets up tables, views and triggers
        :returns: None
        """
        self._create_tables_views_triggers()

