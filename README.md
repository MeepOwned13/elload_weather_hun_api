# Hungarian Electricity load and Weather API

This API will provides 10 minute updates on electricity load and weather for Hungary and provides electricity load forecasts as well using AI.

Refer to [OMSZ Terms and Conditions](https://odp.met.hu/ODP_altalanos_felhasznalasi_feltetelek.pdf) on how to use the data this project downloads, manages in a Database.

Refer to MAVIR publication guide on [MAVIR SystemLoad](https://mavir.hu/web/mavir/rendszerterheles) on how to use electricity data.

## Installation

```bash
python -m venv .venv
".venv/Scripts/activate"
pip install -r requirements.txt
```

Specify MySQL database connection data in .env file.

```bash
# .env example
DB_HOST = localhost
DB_USER = sebok
DB_PASS = sebokpassword
DB_NAME = hunelwapi
```

## Usage

```bash
python src/main.py
```

Switches:
- -h, --help: Show help message and exit
- -sc, --skip_checks: Skip initial check for new data, used for restarting
- -d, --dev: Run in development mode, no updates happen, internet connection not needed

