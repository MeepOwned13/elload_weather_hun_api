import logging
import argparse
from pathlib import Path
import library.omsz_downloader as o_dl
import library.mavir_downloader as m_dl
import library.reader as rd
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi_utils.tasks import repeat_every

logger = logging.getLogger("app")
db_path = Path(f"{__file__}/../../data/sqlite.db").resolve()
omsz_dl = o_dl.OMSZ_Downloader(db_path)
mavir_dl = m_dl.MAVIR_Downloader(db_path)
reader = rd.Reader(db_path)

OMSZ_MESSAGE = "Weather data is from OMSZ, source: (https://odp.met.hu/)"
MAVIR_MESSAGE = "Electricity data is from MAVIR, source: (https://mavir.hu/web/mavir/rendszerterheles)"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Started")
    await update_check()
    yield
    logger.info("Finished")

app = FastAPI(lifespan=lifespan)


@repeat_every(seconds=10)
async def update_check():
    logger.info("Checking for updates to data sources")
    omsz_dl.choose_curr_update()
    mavir_dl.choose_update()


@app.get("/")
async def index():
    return {"Message": f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}"}


@app.get("/omsz_meta/")
async def get_omsz_meta():
    df = reader.get_weather_meta()
    return {"Message": OMSZ_MESSAGE, "data": df.to_json()}


def main(logger: logging.Logger, skip_checks: bool):
    # Setup, define variables, assign classes
    logger.debug("Setting up")
    (db_path / "..").resolve().mkdir(exist_ok=True)
    # OMSZ init
    if not skip_checks:
        omsz_dl.startup_sequence()
    # MAVIR init
    if not skip_checks:
        mavir_dl.update_electricity_data()

    # Start the app
    uvicorn.run(app, port=8000)


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="HUN Electricity and Weather API")
    parser.add_argument("-sc", "--skip_checks", help="Skip startup DB download checks",
                        action="store_true")
    arg = parser.parse_args()

    # Set up logging
    log_folder = Path(f"{__file__}/../../logs").resolve()
    log_folder.mkdir(exist_ok=True)

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
    rd.reader_logger.addHandler(log_fh)

    logger.addHandler(log_ch)
    o_dl.omsz_downloader_logger.addHandler(log_ch)
    m_dl.mavir_downloader_logger.addHandler(log_ch)
    rd.reader_logger.addHandler(log_ch)

    main(logger, arg.skip_checks)

