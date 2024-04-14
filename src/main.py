import logging
import argparse
from pathlib import Path
import library.omsz_downloader as o_dl
import library.mavir_downloader as m_dl
import library.reader as rd
import library.ai_integrator as ai
import pandas as pd
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Response, Request
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
from warnings import filterwarnings
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# Known Warning in Reader and AIIntegrator, all cases that are required tested and working
filterwarnings("ignore", category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')

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

log_config = Path(f"{__file__}/../../logs/log.ini").resolve().absolute().as_posix()
logger = logging.getLogger("app")
omsz_dl = o_dl.OMSZDownloader(db_connect_info)
mavir_dl = m_dl.MAVIRDownloader(db_connect_info)
reader = rd.Reader(db_connect_info)
ai_int = ai.AIIntegrator(db_connect_info, Path(f"{__file__}/../../models").resolve())
last_weather_update: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
last_electricity_update: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None)
# S2S needs 10 minutes removed, because omsz is in delay (-> at 14:05:00 the update for 14:00:00 cannot happen)
last_s2s_update: pd.Timestamp = pd.Timestamp.now("UTC").tz_localize(None) - pd.DateOffset(minutes=10)

TITLE = "HUN EL&W API"
FAVICON_PATH = Path(f"{__file__}/../favicon.ico").resolve()
DEV_MODE = False
OMSZ_MESSAGE = "Weather data is from OMSZ, source: (https://odp.met.hu/)"
MAVIR_MESSAGE = "Electricity data is from MAVIR, source: (https://mavir.hu/web/mavir/rendszerterheles)"
DEFAULT_TO_JSON = {
    "orient": "index",
    "date_format": "iso",
    "date_unit": "s"
}


def df_json_resp(message: str, df: pd.DataFrame):
    """
    Allowing FastAPI to convert to JSON results in slow conversions with large data
    :param message: Message field in response
    :param df: pandas DataFrame to convert to json
    :returns: Response where output JSON is {"Message": message, "data": json_df}
    """
    data = df.replace({np.nan: None}).to_json(**DEFAULT_TO_JSON)
    return Response(content=f'{{"Message": "{message}", "data": {data}}}',
                    media_type="application/json")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Started")
    await update_check()
    yield
    logger.info("Finished")

limiter = Limiter(key_func=get_remote_address)
# Rate limited functions require the request: Request argument as their first

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

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@repeat_every(seconds=10)
async def update_check():
    if DEV_MODE:
        return
    now = pd.Timestamp.now("UTC").tz_localize(None)
    try:
        global last_weather_update
        # Check if we have updated in this 10 min time period
        if now.floor('10min') != last_weather_update.floor('10min'):
            logger.info("Checking for updates to omsz sources")
            if omsz_dl.choose_curr_update():
                last_weather_update = pd.Timestamp.now("UTC").tz_localize(None)
                reader.refresh_caches(["omsz", "ai"])
    except Exception as e:
        logger.error(f"Exception/Error {e.__class__.__name__} occured during OMSZ update, "
                     f"Changes were rolled back, resuming app | message: {str(e)} | "
                     f"Make sure you are connected to the internet and https://odp.met.hu/ is available")
    try:
        global last_electricity_update
        # Check if we have updated in this 10 min time period
        if now.floor('10min') != last_electricity_update.floor('10min'):
            logger.info("Checking for updates to mavir sources")
            if mavir_dl.choose_update():
                last_electricity_update = pd.Timestamp.now("UTC").tz_localize(None)
                reader.refresh_caches(["mavir", "ai"])
    except Exception as e:
        logger.error(f"Exception/Error {e.__class__.__name__} occured during MAVIR update, "
                     f"Changes were rolled back, resuming app | message: {str(e)} | "
                     f"Make sure you are connected to the internet and https://www.mavir.hu is available")
    # Change rollback is provided by the respective classes

    # Check if we updated in this hour and the data is available (omsz and mavir have already updated)
    # Looking at -10 minutes for weather since omsz data is delayed by 10 minutes
    try:
        global last_s2s_update
        if now.floor('h') != last_s2s_update.floor('h') and\
           now.floor('h') == (last_weather_update - pd.DateOffset(minutes=10)).floor('h') and\
           now.floor('h') == last_electricity_update.floor('h'):
            logger.info("Updating S2S predictions")
            if ai_int.choose_update():
                last_s2s_update = pd.Timestamp.now("UTC").tz_localize(None)
                reader.refresh_caches("s2s")
    except Exception as e:
        logger.error(f"Exception/Error {e.__class__.__name__} occured during S2S update, "
                     f"Changes were rolled back, resuming app | message: {str(e)}")


@app.get('/favicon.ico', include_in_schema=False)
async def favicon(request: Request):
    return FileResponse(FAVICON_PATH)


@app.get("/docs", include_in_schema=False)
async def swagger_ui_html(request: Request):
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title=TITLE,
        swagger_favicon_url="/favicon.ico"
    )


@app.get("/redoc", include_in_schema=False)
async def overridden_redoc(request: Request):
    return get_redoc_html(
        openapi_url="/openapi.json",
        title="FastAPI",
        redoc_favicon_url="/favicon.ico"
    )


@app.get("/", responses=response_examples['/'])
@limiter.limit("2/second")
async def index(request: Request):
    """
    Get message about usage and sources from OMSZ and MAVIR, and last update times
    """
    return {"Message": f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}", "last_omsz_update": last_weather_update,
            "last_mavir_update": last_electricity_update, "last_s2s_update": last_s2s_update}


@app.get("/omsz/logo", responses=response_examples['/omsz/logo'])
@limiter.limit("2/second")
async def get_omsz_logo(request: Request):
    """
    Get url to OMSZ logo required when displaying OMSZ data visually.
    """
    return "https://www.met.hu/images/logo/omsz_logo_1362x492_300dpi.png"


@app.get("/omsz/meta", responses=response_examples["/omsz/meta"])
@limiter.limit("1/second")
async def get_omsz_meta(request: Request):
    """
    Retrieve the metadata for Weather/OMSZ stations
    Contains info about the stations' location
    """
    result: pd.DataFrame = reader.get_weather_meta()
    return df_json_resp(OMSZ_MESSAGE, result)


@app.get("/omsz/status", responses=response_examples["/omsz/status"])
@limiter.limit("1/second")
async def get_omsz_status(request: Request):
    """
    Retrieve the status for Weather/OMSZ stations
    Contains info about the stations' location, Start and End dates of observations
    """
    result: pd.DataFrame = reader.get_weather_status()
    return df_json_resp(OMSZ_MESSAGE, result)


@app.get("/omsz/columns", responses=response_examples["/omsz/columns"])
@limiter.limit("1/second")
async def get_omsz_columns(request: Request):
    """
    Get the columns available in weather data paired with the measurement units
    """
    result = {name: omsz_dl.units[name] for name in reader.get_weather_columns()}
    return {"Message": OMSZ_MESSAGE, "data": result}


@app.get("/omsz/weather", responses=response_examples["/omsz/weather"])
@limiter.limit("1/2second")
async def get_weather_station(request: Request, start_date: datetime, end_date: datetime,
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

    When retrieving a single station limit of timeframe is 4 years

    When retrieving more than 1 station the limit of timeframe is 1 week

    Time is used as a key and will be returned no matter if it's in the specified columns
    """
    try:
        if not station:
            station = []
        if len(station) == 1:
            result: pd.DataFrame = reader.get_weather_stations(start_date, end_date, cols=col, stations=station)
            result.drop(columns="StationNumber", inplace=True, errors="ignore")
            result.set_index("Time", inplace=True, drop=True)
            return df_json_resp(OMSZ_MESSAGE, result)

        # multi-station, also returning a response directly, showed to be faster
        df: pd.DataFrame = reader.get_weather_stations(start_date, end_date, cols=col, stations=station)
        result = []
        group_name = "Time" if date_first else "StationNumber"
        grouped = df.groupby(group_name)
        for gr in grouped.groups:
            group = grouped.get_group(gr).set_index("StationNumber" if date_first else "Time")
            data = group.drop(columns=group_name).replace({np.nan: None}).to_json(**DEFAULT_TO_JSON)
            # str() and replace is for when Time group happens, only thing needed for ISO format at this point
            result.append(f'"{str(gr).replace(" ", "T")}": {data}')
        return Response(content=f'{{"Message": "{OMSZ_MESSAGE}", "data": {{ {", ".join(result)} }} }}',
                        media_type="application/json")
    except (LookupError, ValueError) as error:
        raise HTTPException(status_code=400, detail=str(error))


@app.get("/mavir/logo", responses=response_examples['/mavir/logo'])
@limiter.limit("1/second")
async def get_mavir_logo(request: Request):
    """
    Get url to MAVIR logo to use when displaying MAVIR data visually (optional)
    """
    return "https://www.mavir.hu/o/mavir-portal-theme/images/mavir_logo_white.png"


@app.get("/mavir/status", responses=response_examples["/mavir/status"])
@limiter.limit("1/second")
async def get_mavir_status(request: Request):
    """
    Retrieve the status of Electricity/MAVIR data
    Contains info about each column of the electricity data, specifying the first and last date they are available
    """
    result: pd.DataFrame = reader.get_electricity_status()
    return df_json_resp(MAVIR_MESSAGE, result)


@app.get("/mavir/columns", responses=response_examples["/mavir/columns"])
@limiter.limit("1/second")
async def get_electricity_columns(request: Request):
    """
    Retrieve the columns of electricity data
    """
    result = {name: mavir_dl.units[name] for name in reader.get_electricity_columns()}
    return {"Message": MAVIR_MESSAGE, "data": result}


@app.get("/mavir/load", responses=response_examples["/mavir/load"])
@limiter.limit("1/2second")
async def get_electricity_load(request: Request, start_date: datetime, end_date: datetime,
                               col: Annotated[list[str] | None, Query()] = None):
    """
    Retrieve electricity data
    - **start_date**: Date to start from
    - **end_date**: Date to end on
    - **col**: List of columns to retrieve or nothing to get all columns

    Time is used as a key and will be returned no matter if it's in the specified columns
    """
    try:
        result: pd.DataFrame = reader.get_electricity_load(start_date, end_date, cols=col)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    return df_json_resp(MAVIR_MESSAGE, result)


@app.get("/ai/columns", responses=response_examples["/ai/columns"])
@limiter.limit("1/second")
async def get_ai_columns(request: Request):
    """
    Retrieve the columns of ai table(s)
    """
    result = {name: ai_int.units[name] for name in reader.get_ai_table_columns()}
    return {"Message": f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}", "data": result}


@app.get("/ai/table", responses=response_examples["/ai/table"])
@limiter.limit("1/2second")
async def get_ai_table(request: Request, start_date: pd.Timestamp | datetime | None = None,
                       end_date: pd.Timestamp | datetime | None = None, which: str = '10min'):
    """
    Retrieve AI time-series ready table
    - **start_date**: Date to start from, if unspecified starts at earliest
    - **end_date**: Date to end on, if unspecified starts at latest
    - **which**: aggregation level, one of '10min', '1hour'
    """
    try:
        result: pd.DataFrame = reader.get_ai_table(start_date, end_date, which)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    return df_json_resp(f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}", result)


@app.get("/ai/s2s/status", responses=response_examples["/ai/s2s/status"])
@limiter.limit("1/second")
async def get_s2s_status(request: Request):
    """
    Retrieve the status for S2S predictions
    Contains info about the Start and End dates of predictions
    (relevant to when prediction were made, not for what date)
    """
    result: pd.DataFrame = reader.get_s2s_status()
    return df_json_resp(f"{OMSZ_MESSAGE}, {MAVIR_MESSAGE}", result)


@app.get("/ai/s2s/preds", responses=response_examples["/ai/s2s/preds"])
@limiter.limit("1/2second")
async def get_s2s_preds(request: Request, start_date: pd.Timestamp | datetime | None = None,
                        end_date: pd.Timestamp | datetime | None = None, aligned: bool = False):
    """
    Retrieve predictions of Seq2Seq model
    - **start_date**: Date to start from, if unspecified starts at earliest
    - **end_date**: Date to end on, if unspecified starts at latest
    - **aligned**: align true-pred or just return predictions at time
    """
    try:
        result: pd.DataFrame = reader.get_s2s_preds(start_date, end_date, aligned)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error))
    return df_json_resp(f"About the data used for prediction: {OMSZ_MESSAGE}, {MAVIR_MESSAGE}", result)


def main(skip_checks: bool):
    # Setup, define variables, assign classes
    logger.debug("Setting up")
    # OMSZ init
    if not skip_checks and not DEV_MODE:
        try:
            omsz_dl.startup_sequence()
            global last_weather_update
            last_weather_update = pd.Timestamp.now("UTC").tz_localize(None)
        except Exception as e:
            logger.error(f"Exception/Error {e.__class__.__name__} occured during OMSZ startup sequece, "
                         f"message: {str(e)} | "
                         f"Make sure you are connected to the internet and https://odp.met.hu/ is available")
            exit(1)

    # MAVIR init
    if not skip_checks and not DEV_MODE:
        try:
            mavir_dl.startup_sequence()
            global last_electricity_update
            last_electricity_update = pd.Timestamp.now("UTC").tz_localize(None)
        except Exception as e:
            logger.error(f"Exception/Error {e.__class__.__name__} occured during MAVIR startup sequece, "
                         f"message: {str(e)} | "
                         f"Make sure you are connected to the internet and https://www.mavir.hu is available")
            exit(1)

    # AI init
    ai_int.startup_sequence()
    global last_s2s_update
    last_s2s_update = pd.Timestamp.now("UTC").tz_localize(None) - pd.DateOffset(minutes=10)

    # Cache init
    reader.refresh_caches(["mavir", "omsz", "ai", "s2s"])

    # Start the app
    uvicorn.run(app, port=8000, log_config=log_config)


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="HUN Electricity and Weather API")
    parser.add_argument("-sc", "--skip_checks", help="Skip startup DB download checks", action="store_true")
    parser.add_argument("-d", "--dev", help="Developer mode, no downloads happen", action="store_true")
    args = parser.parse_args()

    DEV_MODE = args.dev

    # Set up logging
    logging.config.fileConfig(log_config, disable_existing_loggers=False)

    main(args.skip_checks)

