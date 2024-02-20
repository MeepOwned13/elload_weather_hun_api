import logging
import argparse
from pathlib import Path
import library.omsz_downloader as o_dl
import library.mavir_downloader as m_dl
import library.reader as rd
import pandas as pd
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi_utils.tasks import repeat_every
from datetime import datetime
import numpy as np

logger = logging.getLogger("app")
db_path = Path(f"{__file__}/../../data/sqlite.db").resolve()
omsz_dl = o_dl.OMSZ_Downloader(db_path)
mavir_dl = m_dl.MAVIR_Downloader(db_path)
reader = rd.Reader(db_path)

DEV_MODE = False
OMSZ_MESSAGE = "Weather data is from OMSZ, source: (https://odp.met.hu/)"
MAVIR_MESSAGE = "Electricity data is from MAVIR, source: (https://mavir.hu/web/mavir/rendszerterheles)"
DEFAULT_TO_DICT = {
    "orient": "index",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Started")
    await update_check()
    yield
    logger.info("Finished")

app = FastAPI(lifespan=lifespan)


@repeat_every(seconds=30)
async def update_check():
    if DEV_MODE:
        return
    logger.info("Checking for updates to data sources")
    omsz_dl.choose_curr_update()
    mavir_dl.choose_update()


@app.get("/")
async def index():
    return {"Message": f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}"}


@app.get("/omsz/meta/")
async def get_omsz_meta():
    df: pd.DataFrame = reader.get_weather_meta()
    return {"Message": OMSZ_MESSAGE, "data": df.to_dict(**DEFAULT_TO_DICT)}


@app.get("/omsz/station")
async def get_weather_station(station: int, start_date: datetime, end_date: datetime):
    try:
        df: pd.DataFrame = reader.get_weather_station(station, start_date, end_date)
    except (LookupError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"Message": OMSZ_MESSAGE, "data": df.replace({np.nan: None}).to_dict(**DEFAULT_TO_DICT)}


@app.get("/omsz/all")
async def get_weather_time(start_date: datetime, end_date: datetime):
    try:
        result: dict = reader.get_weather_time(start_date, end_date, df_to_dict=DEFAULT_TO_DICT)
    except (LookupError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"Message": OMSZ_MESSAGE, "data": result}


@app.get("/mavir/meta/")
async def get_mavir_meta():
    df: pd.DataFrame = reader.get_electricity_meta()
    return {"Message": MAVIR_MESSAGE, "data": df.to_dict(**DEFAULT_TO_DICT)}


def main(logger: logging.Logger, skip_checks: bool):
    # Setup, define variables, assign classes
    logger.debug("Setting up")
    (db_path / "..").resolve().mkdir(exist_ok=True)
    # OMSZ init
    if not skip_checks and not DEV_MODE:
        omsz_dl.startup_sequence()
    # MAVIR init
    if not skip_checks and not DEV_MODE:
        mavir_dl.update_electricity_data()

    # Start the app
    uvicorn.run(app, port=8000)


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="HUN Electricity and Weather API")
    parser.add_argument("-sc", "--skip_checks", help="Skip startup DB download checks", action="store_true")
    parser.add_argument("-d", "--dev", help="Developer mode, no downloads happen", action="store_true")
    args = parser.parse_args()

    DEV_MODE = args.dev

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

    main(logger, args.skip_checks)

