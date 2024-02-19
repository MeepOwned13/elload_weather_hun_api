import logging
from pathlib import Path
import library.omsz_downloader as o_dl
import library.mavir_downloader as m_dl


def main(logger: logging.Logger):
    # Setup, define variables, assign classes
    logger.debug("Setting up")
    db_path: Path = Path(f"{__file__}/../../data/sqlite.db").resolve()
    (db_path / "..").resolve().mkdir(exist_ok=True)
    # OMSZ init
    omsz_dl = o_dl.OMSZ_Downloader(db_path)
    omsz_dl.startup_sequence()
    # MAVIR init
    mavir_dl = m_dl.MAVIR_Downloader(db_path)
    mavir_dl.update_electricity_data()

    # Start the app
    logger.info("Started")

    omsz_dl.choose_curr_update()
    mavir_dl.choose_update()

    logger.info("Finished")


if __name__ == "__main__":
    # Set up logging
    log_folder = Path(f"{__file__}/../../logs").resolve()
    log_folder.mkdir(exist_ok=True)

    logger = logging.getLogger("app")
    logger.setLevel(logging.DEBUG)

    log_fh = logging.FileHandler(log_folder / "app.log")
    log_fh.setLevel(logging.DEBUG)

    log_ch = logging.StreamHandler()
    log_ch.setLevel(logging.INFO)

    log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    log_fh.setFormatter(log_format)
    log_ch.setFormatter(log_format)

    # Start loggers
    logger.addHandler(log_fh)
    o_dl.omsz_downloader_logger.addHandler(log_fh)
    m_dl.mavir_downloader_logger.addHandler(log_fh)

    logger.addHandler(log_ch)
    o_dl.omsz_downloader_logger.addHandler(log_ch)
    m_dl.mavir_downloader_logger.addHandler(log_ch)

    main(logger)

