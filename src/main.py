import logging
import argparse
from pathlib import Path
import library.omsz_downloader as o_dl
import library.mavir_downloader as m_dl
import library.reader as rd
import pandas as pd
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi_utils.tasks import repeat_every
from fastapi.responses import FileResponse
from fastapi.openapi.docs import get_swagger_ui_html, get_redoc_html
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import numpy as np
from typing import Annotated
from response_examples import response_examples
from dotenv import dotenv_values
import mysql.connector as connector

db_connect_info = dotenv_values(".env")
db_connect_info = {
    "host": db_connect_info["HOST"],
    "user": db_connect_info["USER"],
    "password": db_connect_info["PASW"],
    "database": db_connect_info["DBNM"]
}
# Setup DB, need to create DB with DBNM if it don't exist yet
try:
    conn = connector.connect(**db_connect_info)
except connector.errors.ProgrammingError:
    conn = connector.connect(host=db_connect_info["host"], user=db_connect_info["user"],
                             password=db_connect_info["password"])
    c = conn.cursor()
    c.execute(f"CREATE DATABASE {db_connect_info['database']}")
conn.close()

logger = logging.getLogger("app")
omsz_dl = o_dl.OMSZ_Downloader(db_connect_info)
mavir_dl = m_dl.MAVIR_Downloader(db_connect_info)
reader = rd.Reader(db_connect_info)
last_weather_update: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
last_electricity_update: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)

TITLE = "HUN EL&W API"
FAVICON_PATH = Path(f"{__file__}/../favicon.ico").resolve()
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

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    title=TITLE,
    summary="Hungary Electricity Load and Weather API",
    description="Get live updates of Hungary's National Electricity Load and Weather stations",
    version="1.0.0",
    lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_headers=["*"],
)


@repeat_every(seconds=10)
async def update_check():
    if DEV_MODE:
        return
    logger.info("Checking for updates to data sources")
    try:
        if omsz_dl.choose_curr_update():
            global last_weather_update
            last_weather_update = pd.Timestamp.now("UTC").tz_localize(None)
    except Exception as e:
        logger.error(f"Exception/Error {e.__class__.__name__} occured during OMSZ update, "
                     f"Changes were rolled back, resuming app"
                     f"message: {str(e)} | "
                     f"Make sure you are connected to the internet and https://odp.met.hu/ is available")
    try:
        if mavir_dl.choose_update():
            global last_electricity_update
            last_electricity_update = pd.Timestamp.now("UTC").tz_localize(None)
    except Exception as e:
        logger.error(f"Exception/Error {e.__class__.__name__} occured during MAVIR update, "
                     f"Changes were rolled back, resuming app"
                     f"message: {str(e)} | "
                     f"Make sure you are connected to the internet and https://www.mavir.hu is available")
    # Change rollback is provided by the respective classes


@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse(FAVICON_PATH)


@app.get("/docs", include_in_schema=False)
async def swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title=TITLE,
        swagger_favicon_url="/favicon.ico"
    )


@app.get("/redoc", include_in_schema=False)
def overridden_redoc():
    return get_redoc_html(
        openapi_url="/openapi.json",
        title="FastAPI",
        redoc_favicon_url="/favicon.ico"
    )


@app.get("/", responses=response_examples['/'])
async def index():
    """
    Get message about usage and sources from OMSZ and MAVIR, and last update times
    """
    return {"Message": f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}", "last_omsz_update": last_weather_update,
            "last_mavir_update": last_electricity_update}


@app.get("/omsz/logo", responses=response_examples['/omsz/logo'])
async def get_omsz_logo():
    """
    Get url to OMSZ logo required when displaying OMSZ data visually.
    """
    return "https://www.met.hu/images/logo/omsz_logo_1362x492_300dpi.png"


@app.get("/omsz/meta", responses=response_examples["/omsz/meta"])
async def get_omsz_meta():
    """
    Retrieve the metadata for Weather/OMSZ stations
    Contains info about the stations' location
    """
    df: pd.DataFrame = reader.get_weather_meta()
    return {"Message": OMSZ_MESSAGE, "data": df.to_dict(**DEFAULT_TO_DICT)}


@app.get("/omsz/status", responses=response_examples["/omsz/status"])
async def get_omsz_status():
    """
    Retrieve the status for Weather/OMSZ stations
    Contains info about the stations' location, Start and End dates of observations
    """
    df: pd.DataFrame = reader.get_weather_status()
    return {"Message": OMSZ_MESSAGE, "data": df.to_dict(**DEFAULT_TO_DICT)}


@app.get("/omsz/columns", responses=response_examples["/omsz/columns"])
async def get_omsz_columns():
    """
    Get the columns available in weather data
    """
    result = reader.get_weather_columns()
    return {"Message": OMSZ_MESSAGE, "data": result}


@app.get("/omsz/weather", responses=response_examples["/omsz/weather"])
async def get_weather_station(start_date: datetime, end_date: datetime,
                              station: Annotated[list[int] | None, Query()] = None,
                              col: Annotated[list[str] | None, Query()] = None,
                              date_first: bool = False):
    """
    Retrieve weather data
    - **start_date**: Date to start from
    - **end_date**: Date to end on
    - **station**: List of stations to retrieve or nothing to get all stations
    - **col**: List of columns to retrieve or nothing to get all columns
    - **date_first**: On multistation query, results are grouped by date instead of station

    When retrieving a single station
    - limit of timeframe is 5 years
    - col(s) get checked for existence, error if no columns are valid

    When retrieving more than 1 station
    - limit of timeframe is 1 week
    - col(s) get checked but return no errors, they are only left out of the result

    Time is used as a key and will be returned no matter if it's in the specified columns
    """
    if not station:
        station = []
    try:
        if len(station) == 1:
            df: pd.DataFrame = reader.get_weather_one_station(station[0], start_date, end_date, cols=col)
            result = df.replace({np.nan: None}).to_dict(**DEFAULT_TO_DICT)
        else:
            df: pd.DataFrame = reader.get_weather_multi_station(start_date, end_date, cols=col, stations=station)
            result = {}
            group_name = "Time" if date_first else "StationNumber"
            grouped = df.groupby(group_name)
            for gr in grouped.groups:
                group = grouped.get_group(gr).set_index("StationNumber" if date_first else "Time")
                result[gr] = group.drop(columns=group_name).replace({np.nan: None}).to_dict(**DEFAULT_TO_DICT)
    except (LookupError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"Message": OMSZ_MESSAGE, "data": result}


@app.get("/mavir/logo", responses=response_examples['/mavir/logo'])
async def get_mavir_logo():
    """
    Get url to MAVIR logo to use when displaying MAVIR data visually (optional)
    """
    return "https://www.mavir.hu/o/mavir-portal-theme/images/mavir_logo_white.png"


@app.get("/mavir/status", responses=response_examples["/mavir/status"])
async def get_mavir_meta():
    """
    Retrieve the status of Electricity/MAVIR data
    Contains info about each column of the electricity data, specifying the first and last date they are available
    """
    df: pd.DataFrame = reader.get_electricity_meta()
    return {"Message": MAVIR_MESSAGE, "data": df.to_dict(**DEFAULT_TO_DICT)}


@app.get("/mavir/columns", responses=response_examples["/mavir/columns"])
async def get_electricity_columns():
    """
    Retrieve the columns of electricity data
    """
    result = reader.get_electricity_columns()
    return {"Message": MAVIR_MESSAGE, "data": result}


@app.get("/mavir/load", responses=response_examples["/mavir/load"])
async def get_electricity_load(start_date: datetime, end_date: datetime,
                               col: Annotated[list[str] | None, Query()] = None):
    """
    Retrieve electricity data
    - **start_date**: Date to start from
    - **end_date**: Date to end on
    - **col**: List of columns to retrieve or nothing to get all columns

    Time is used as a key and will be returned no matter if it's in the specified columns
    """
    try:
        df: pd.DataFrame = reader.get_electricity_load(start_date, end_date, cols=col)
        result = df.replace({np.nan: None}).to_dict(**DEFAULT_TO_DICT)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"Message": MAVIR_MESSAGE, "data": result}


def main(logger: logging.Logger, skip_checks: bool):
    # Setup, define variables, assign classes
    logger.debug("Setting up")
    # OMSZ init
    if not skip_checks and not DEV_MODE:
        try:
            omsz_dl.startup_sequence()
        except Exception as e:
            logger.error(f"Exception/Error {e.__class__.__name__} occured during OMSZ startup sequece, "
                         f"message: {str(e)} | "
                         f"Make sure you are connected to the internet and https://odp.met.hu/ is available")
            exit(1)
    # MAVIR init
    if not skip_checks and not DEV_MODE:
        try:
            mavir_dl.startup_sequence()
        except Exception as e:
            logger.error(f"Exception/Error {e.__class__.__name__} occured during MAVIR startup sequece, "
                         f"message: {str(e)} | "
                         f"Make sure you are connected to the internet and https://www.mavir.hu is available")
            exit(1)

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

