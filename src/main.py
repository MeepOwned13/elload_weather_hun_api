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
from datetime import datetime
import numpy as np
from typing import Annotated

logger = logging.getLogger("app")
db_path = Path(f"{__file__}/../../data/sqlite.db").resolve()
omsz_dl = o_dl.OMSZ_Downloader(db_path)
mavir_dl = m_dl.MAVIR_Downloader(db_path)
reader = rd.Reader(db_path)
last_weather_update: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
last_electricity_update: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)


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
    title="HUN EL&W API",
    summary="Hungary Electricity Load and Weather API",
    description="Get live updates of Hungary's National Electricity Load and Weather stations",
    version="1.0.0",
    lifespan=lifespan)


@repeat_every(seconds=30)
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


@app.get("/",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "example": {
                             "Message": "message",
                             "last_omsz_update": "2024-02-23T11:29:56.031130",
                             "last_mavir_update": "2024-02-23T11:29:56.031130"
                         }
                     }
                 }
             }
         })
async def index():
    """
    Get message about usage and sources from OMSZ and MAVIR, and last update times
    """
    return {"Message": f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}", "last_omsz_update": last_weather_update,
            "last_mavir_update": last_electricity_update}


@app.get("/omsz/logo",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "example": {
                             "https://www.met.hu/images/logo/omsz_logo_1362x492_300dpi.png"
                         }
                     }
                 }
             }
         })
async def get_omsz_logo():
    """
    Get url to OMSZ logo required when displaying OMSZ data visually.
    """
    return "https://www.met.hu/images/logo/omsz_logo_1362x492_300dpi.png"


@app.get("/omsz/meta",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "example": {
                             "Message": "string",
                             "data": {
                                 13704: {
                                     "StartDate": "2005-07-27 18:10:00",
                                     "EndDate": "2024-02-21 18:30:00",
                                     "Latitude": 47.6783,
                                     "Longitude": 16.6022,
                                     "Elevation": 232.8,
                                     "StationName": "Sopron Kuruc-domb",
                                     "RegioName": "Győr-Moson-Sopron"
                                 },
                                 13711: {
                                     "...": "..."
                                 }
                             }
                         }
                     }
                 }
             }
         })
async def get_omsz_meta():
    """
    Retrieve the metadata for Weather/OMSZ stations
    Contains info about stations location, start and end date for available data
    """
    df: pd.DataFrame = reader.get_weather_meta()
    return {"Message": OMSZ_MESSAGE, "data": df.to_dict(**DEFAULT_TO_DICT)}


@app.get("/omsz/columns",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "examples": {
                             "Specified Station": {
                                 "value": {
                                     "Message": "string",
                                     "data": {
                                         0: "Time",
                                         1: "Prec",
                                         2: "Temp",
                                         "...": "..."
                                     }
                                 }
                             },
                             "Unspecified Station": {
                                 "value": {
                                     "Message": "string",
                                     "data": {
                                         13704: {
                                             0: "Time",
                                             1: "Prec",
                                             2: "Temp",
                                             "...": "..."
                                         }
                                     }
                                 }
                             }
                         }
                     }
                 }
             },
             400: {
                 "description": "Bad Request",
                 "content": {
                     "application/json": {
                         "example": {
                             "detail": "Error message"
                         }
                     }
                 }
             }
         })
async def get_omsz_columns(station: int | None = None):
    """
    Get the columns of a given station's or all stations' data
    - **station**: single number or nothing to get all stations
    """
    try:
        if station:
            result = reader.get_weather_station_columns(station)
        else:
            result = reader.get_weather_all_columns()
    except (LookupError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"Message": OMSZ_MESSAGE, "data": result}


@app.get("/omsz/weather",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "examples": {
                             "Specified Station": {
                                 "value": {
                                     "Message": "string",
                                     "data": {
                                         "2024-02-18 15:00:00": {
                                             "Prec": 0,
                                             "Temp": 10.7,
                                             "...": "..."
                                         },
                                         "2024-02-18 15:10:00": {
                                             "...": "..."
                                         },
                                         "...": "..."
                                     }
                                 }
                             },
                             "Unspecified Station": {
                                 "value": {
                                     "Message": "string",
                                     "data": {
                                         13704: {
                                             "2024-02-18 15:00:00": {
                                                 "Prec": 0,
                                                 "Temp": 10.7,
                                                 "...": "..."
                                             },
                                             "2024-02-18 15:10:00": {
                                                 "..."
                                             }
                                         },
                                         13711: {
                                             "..."
                                         },
                                         "...": "..."
                                     }
                                 }
                             }
                         }
                     }
                 }
             },
             400: {
                 "description": "Bad Request",
                 "content": {
                     "application/json": {
                         "example": {
                             "detail": "Error message"
                         }
                     }
                 }
             }
         })
async def get_weather_station(start_date: datetime, end_date: datetime,
                              station: Annotated[list[int] | None, Query()] = None,
                              col: Annotated[list[str] | None, Query()] = None):
    """
    Retrieve weather data
    - **start_date**: Date to start from
    - **end_date**: Date to end on
    - **station**: List of stations to retrieve or nothing to get all stations
    - **col**: List of columns to retrieve or nothing to get all columns

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
            df: pd.DataFrame = reader.get_weather_station(station[0], start_date, end_date, cols=col)
            result = df.replace({np.nan: None}).to_dict(**DEFAULT_TO_DICT)
        else:
            result = reader.get_weather_time(start_date, end_date, cols=col,
                                             stations=station, df_to_dict=DEFAULT_TO_DICT)
    except (LookupError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))
    return {"Message": OMSZ_MESSAGE, "data": result}


@app.get("/mavir/meta",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "example": {
                             "Message": "string",
                             "data": {
                                 "NetPlanSystemProduction": {
                                     "StartDate": "2011-11-01 23:10:00",
                                     "EndDate": "2024-02-22 18:50:00",
                                 },
                                 "NetSystemLoad": {
                                     "...": "..."
                                 },
                                 "...": "..."
                             }
                         }
                     }
                 }
             }
         })
async def get_mavir_meta():
    """
    Retrieve the metadata for Electricity/MAVIR data
    Contains info about each column of the electricity data, specifying the first and last date they are available
    """
    df: pd.DataFrame = reader.get_electricity_meta()
    return {"Message": MAVIR_MESSAGE, "data": df.to_dict(**DEFAULT_TO_DICT)}


@app.get("/mavir/columns",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "example": {
                             "Message": "string",
                             "data": {
                                 0: "Time",
                                 1: "NetSystemLoad",
                                 "...": "..."
                             }
                         }
                     }
                 }
             }
         })
async def get_electricity_columns():
    """
    Retrieve the columns of electricity data
    """
    result = reader.get_electricity_columns()
    return {"Message": MAVIR_MESSAGE, "data": result}


@app.get("/mavir/load",
         responses={
             200: {
                 "description": "Succesful Response",
                 "content": {
                     "application/json": {
                         "example": {
                             "Message": "string",
                             "data": {
                                 "2024-02-18 15:00:00": {
                                     "NetSystemLoad": 4717.373,
                                     "NetSystemLoadFactPlantManagment": 4689.369,
                                     "...": "..."
                                 },
                                 "2024-02-18 15:10:00": {
                                     "...": "..."
                                 },
                                 "...": "..."
                             }
                         }
                     }
                 }
             },
             400: {
                 "description": "Bad Request",
                 "content": {
                     "application/json": {
                         "example": {
                             "detail": "Error message"
                         }
                     }
                 }
             }
         })
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
    (db_path / "..").resolve().mkdir(exist_ok=True)
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
            mavir_dl.update_electricity_data()
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

