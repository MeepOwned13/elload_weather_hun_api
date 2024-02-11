import logging
from pathlib import Path
import downloader


def main(logger: logging.Logger):
    # Setup, define variables, assign classes
    logger.debug("Setting up")
    db_path: Path = Path(f"{__file__}/../../data/sqlite.db").resolve()
    omsz_dl = downloader.OMSZ_Downloader(db_path)

    # Start the app
    logger.info("Started")
    # omsz_dl.update_meta()

    logger.info("Finished")


if __name__ == "__main__":
    # Set up logging
    log_folder = Path(f"{__file__}/../../logs").resolve()

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
    downloader.omsz_logger.addHandler(log_fh)

    logger.addHandler(log_ch)
    downloader.omsz_logger.addHandler(log_ch)

    main(logger)

