import logging
from pathlib import Path


def main(logger: logging.Logger):
    # Start app
    logger.debug('Started')
    logger.info('Finished')


if __name__ == '__main__':
    # Set up logging
    log_folder = Path(f"{__file__}/../../logs").resolve()

    logger = logging.getLogger("app")
    logger.setLevel(logging.DEBUG)

    log_fh = logging.FileHandler(log_folder / "app.log")
    log_fh.setLevel(logging.DEBUG)

    log_ch = logging.StreamHandler()
    log_ch.setLevel(logging.INFO)

    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log_fh.setFormatter(log_format)
    log_ch.setFormatter(log_format)

    # Start loggers
    logger.addHandler(log_fh)
    logger.addHandler(log_ch)

    main(logger)

