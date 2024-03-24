import logging
import pandas as pd
from .utils.db_connect import DatabaseConnect

ai_integrator_logger = logging.getLogger("ai")
ai_integrator_logger.setLevel(logging.DEBUG)
ai_integrator_logger.addHandler(logging.NullHandler())


class AIIntegrator(DatabaseConnect):
    """
    Class to update AI tables with data inside given Database
    Relies on OMSZ_data and MAVIR_data tables defined by OMSZDownloader and MAVIRDownlader classes
    CALL startup_sequence() TO CREATE ALL REQUIRED TABLES
    """

    def __init__(self, db_connect_info: dict):
        """
        :param db_connect_info: connection info for MySQL connector
        """
        super().__init__(db_connect_info, ai_integrator_logger)

    def __del__(self):
        super().__del__()

    @DatabaseConnect._db_transaction
    def _create_tables_views_triggers(self):
        """
        Creates necessary data table, aggregate view and maintaning triggers
        """
        self._logger.debug("Starting to create tables, views, triggers that don't exist")
        from_time = "2015-01-01 0:00:00"

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
                WHERE Time > "{from_time}") m
            NATURAL JOIN (
                SELECT Time, SUM(Prec) Prec, AVG(Temp) Temp, AVG(RHum) RHum,
                       AVG(GRad) GRad, AVG(Pres) Pres, AVG(AvgWS) Wind
                FROM OMSZ_data FORCE INDEX(OMSZ_data_time_index)
                WHERE Time > "{from_time}" GROUP BY Time) o
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
               FROM ai_10min
               GROUP BY FROM_UNIXTIME(CEIL(UNIX_TIMESTAMP(Time) / 3600) * 3600)
               HAVING FROM_UNIXTIME(CEIL(UNIX_TIMESTAMP(Time) / 3600) * 3600) <=
                      (SELECT Time FROM ai_10min WHERE Temp IS NOT NULL ORDER BY Time DESC LIMIT 1);
            """
        )

        # AI_10min's consistency is guaranteed by the following triggers on MAVIR_data and OMSZ_data
        mavir_ins_upd =\
            f"""
            BEGIN
               IF NEW.Time > "{from_time}" AND NEW.NetSystemLoad IS NOT NULL THEN
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
               IF OLD.Time > "{from_time}" AND OLD.NetSystemLoad IS NOT NULL THEN
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

               IF NEW.Time > "{from_time}" THEN
                  SELECT SUM(Prec) Prec, AVG(Temp) Temp, AVG(RHum) RHum,
                     AVG(GRad) GRad, AVG(Pres) Pres, AVG(AvgWS) Wind
                  INTO Prec_, Temp_, RHum_, GRad_, Pres_, Wind_
                  FROM omsz_data
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

        self._logger.info("Created tables, views, triggers that didn't exist")

    def startup_sequence(self):
        """
        Sets up tables, views and triggers
        :returns: None
        """
        self._create_tables_views_triggers()

