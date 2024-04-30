import unittest
from fastapi.testclient import TestClient
import httpx
from httpx import Response
import sys
from warnings import filterwarnings
# Following 2 imports' order is very important
sys.path.append("src/")  # this has to be before import main since it's a top level module!!


class ApiTests(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        # this import is moved here, because autoformatting tools put it at the top if it's not under anything
        # this has to be after sys.path.append("src/") since this is a top level module!!
        import main as app

        app.limiter.enabled = False
        app.DEV_MODE = True
        super().__init__(*args, **kwargs)
        self.client = TestClient(app.app)

    def setUp(self):
        # Known Warning in Reader and AIIntegrator, all cases that are required tested and working
        filterwarnings("ignore", category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')

    def client_get(self, path: str) -> Response:
        """
        Get path and test repsonse code 200
        :param path: path to request
        :returns: httpx.Response
        """
        response: Response = self.client.get(path)
        self.assertEqual(response.status_code, 200)
        return response

    def data_get(self, path: str) -> dict:
        """
        Get path, test response 200, test Message and data fields and return repsonse json["data"]
        :param path: path to request
        :returns: dict
        """
        json: dict = self.client_get(path).json()
        self.assertTrue("Message" in json)
        self.assertTrue("data" in json)
        return json["data"]

    def ISO_date_assert(self, date: str):
        """
        Test if date is of ISO format
        :param date: date string to test
        """
        self.assertRegex(date, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    def test_favicon(self):
        # Test favicon availability
        self.client_get("/favicon.ico")

    def test_docs(self):
        # Test docs availability
        self.client_get("/docs")

    def test_redoc(self):
        # Test redoc availability
        self.client_get("/redoc")

    def test_index(self):
        # Test specified response fields
        response: Response = self.client_get("/")
        json = response.json()
        self.assertIn("Message", json)
        self.assertIn("last_omsz_update", json)
        self.assertIn("last_mavir_update", json)
        self.assertIn("last_s2s_update", json)

    def test_omsz_logo(self):
        # Test if omsz logo is valid and available
        response: Response = self.client_get("/omsz/logo")
        logo_response: Response = httpx.get(response.json())
        self.assertEqual(logo_response.status_code, 200)

    def test_omsz_meta(self):
        # Test omsz meta fields
        data: dict = self.data_get("/omsz/meta")
        for record in data.values():
            self.assertIn("Latitude", record)
            self.assertIn("Longitude", record)
            self.assertIn("Elevation", record)
            self.assertIn("StationName", record)
            self.assertIn("RegioName", record)

    def test_omsz_status(self):
        # Test omsz status fields
        data: dict = self.data_get("/omsz/status")
        for record in data.values():
            self.assertIn("Latitude", record)
            self.assertIn("Longitude", record)
            self.assertIn("Elevation", record)
            self.assertIn("StationName", record)
            self.assertIn("RegioName", record)
            self.assertIn("StartDate", record)
            self.assertIn("EndDate", record)

    def test_omsz_columns(self):
        # Test omsz columns response
        data: dict = self.data_get("/omsz/columns")
        # Testing if it's dict to check that it provides measurement units too
        self.assertIsInstance(data, dict)

    def test_omsz_weather_one_station(self):
        # Test single station response
        station: int = list(self.data_get("/omsz/meta").keys())[0]  # Get a station id
        cols: list = list(self.data_get("/omsz/columns").keys())

        data: dict = self.data_get(
            f"/omsz/weather?station={station}&start_date=2024-01-07T12:10:00&end_date=2024-01-08T08:20:00")

        for date, record in data.items():
            self.ISO_date_assert(date)
            for col in record:
                self.assertIn(col, cols)

    def test_omsz_weather_multi_station(self):
        # Test multi station response
        stations: int = list(self.data_get("/omsz/meta").keys())[0:10]  # Get some station ids
        cols: list = list(self.data_get("/omsz/columns").keys())

        str_for_stations: str = "&".join([f"station={s}" for s in stations])
        # This is date_first=False, station numbers come first
        data: dict = self.data_get(
            f"/omsz/weather?{str_for_stations}&start_date=2024-01-07T09:17:00&end_date=2024-01-08T22:15:00")

        for station, s_data in data.items():
            self.assertIn(station, stations)  # Test I wanted to retrieve this id
            for date, record in s_data.items():
                self.ISO_date_assert(date)
                for col in record:
                    self.assertIn(col, cols)

    def test_omsz_weather_date_first(self):
        # Test multi station response with date_first
        stations: int = list(self.data_get("/omsz/meta").keys())[0:10]  # Get some station ids
        cols: list = list(self.data_get("/omsz/columns").keys())

        str_for_stations: str = "&".join([f"station={s}" for s in stations])
        # This is date_first=True, dates come first
        data: dict = self.data_get(f"/omsz/weather?{str_for_stations}"
                                   f"&start_date=2024-01-09T09:56:00&end_date=2024-01-09T22:12:00&date_first=True")

        for date, d_data in data.items():
            self.ISO_date_assert(date)
            for station, record in d_data.items():
                self.assertIn(station, stations)  # Test I wanted to retrieve this id
                for col in record:
                    self.assertIn(col, cols)

    def test_omsz_weather_cols(self):
        # Test multi station response with columns specified
        stations: int = list(self.data_get("/omsz/meta").keys())[0:10]  # Get some station ids
        cols: list = list(self.data_get("/omsz/columns").keys())[0:6]

        str_for_stations: str = "&".join([f"station={s}" for s in stations])
        str_for_cols: str = "&".join([f"col={c}" for c in cols])
        # This is date_first=True, dates come first
        data: dict = self.data_get(f"/omsz/weather?{str_for_stations}&{str_for_cols}"
                                   f"&start_date=2024-01-09T09:34:00&end_date=2024-01-09T22:00:00&date_first=True")

        for date, d_data in data.items():
            self.ISO_date_assert(date)
            for station, record in d_data.items():
                self.assertIn(station, stations)  # Test I wanted to retrieve this id
                for col in record:
                    self.assertIn(col, cols)

    def test_omsz_weather_all_station(self):
        # Test all station response
        # This is date_first=False, station numbers come first
        data: dict = self.data_get("/omsz/weather?start_date=2024-01-12T17:34:00&end_date=2024-01-14T08:42:00")
        cols: list = list(self.data_get("/omsz/columns").keys())

        for station, s_data in data.items():
            for date, record in s_data.items():
                self.ISO_date_assert(date)
                for col in record:
                    self.assertIn(col, cols)

    def test_mavir_logo(self):
        # Test if mavir logo is valid and available
        response: Response = self.client_get("/mavir/logo")
        logo_response: Response = httpx.get(response.json())
        self.assertEqual(logo_response.status_code, 200)

    def test_mavir_status(self):
        # Test mavir status fields
        data: dict = self.data_get("/mavir/status")
        for record in data.values():
            self.assertIn("StartDate", record)
            self.assertIn("EndDate", record)

    def test_mavir_columns(self):
        # Test mavir columns response
        data: dict = self.data_get("/mavir/columns")
        # Testing if it's dict to check that it provides measurement units too
        self.assertIsInstance(data, dict)

    def test_mavir_load(self):
        # Test load response
        cols: list = list(self.data_get("/mavir/columns").keys())
        data: dict = self.data_get("/mavir/load?&start_date=2024-02-17T6:15:00&end_date=2024-02-20T17:43:00")

        for date, d_data in data.items():
            self.ISO_date_assert(date)
            for col in d_data:
                self.assertIn(col, cols)

    def test_mavir_load_cols(self):
        # Test load response with columns specified
        cols: list = list(self.data_get("/mavir/columns").keys())[0:5]

        str_for_cols: str = "&".join([f"col={c}" for c in cols])
        # This is date_first=True, dates come first
        data: dict = self.data_get(
            f"/mavir/load?{str_for_cols}&start_date=2024-02-12T12:04:00&end_date=2024-02-15T21:50:00")

        for date, d_data in data.items():
            self.ISO_date_assert(date)
            for col in d_data:
                self.assertIn(col, cols)

    def test_ai_columns(self):
        # Test ai columns response
        data: dict = self.data_get("/ai/columns")
        # Testing if it's dict to check that it provides measurement units too
        self.assertIsInstance(data, dict)

    def test_ai_table_10min(self):
        # Test AI table 1 hour response
        cols: list = list(self.data_get("/ai/columns").keys())
        data: dict = self.data_get("/ai/table?&start_date=2024-02-23T05:00:00&end_date=2024-02-27T15:09:00&which=10min")

        for date, d_data in data.items():
            self.ISO_date_assert(date)
            for col in d_data:
                self.assertIn(col, cols)

    def test_ai_table_1hour(self):
        # Test AI table 1 hour response
        cols: list = list(self.data_get("/ai/columns").keys())
        data: dict = self.data_get("/ai/table?&start_date=2024-02-17T06:27:00&end_date=2024-02-20T17:13:00&which=1hour")

        for date, d_data in data.items():
            self.ISO_date_assert(date)
            for col in d_data:
                self.assertIn(col, cols)

    def test_s2s_status(self):
        # Test s2s status fields
        data: dict = self.data_get("/ai/s2s/status")
        for record in data.values():
            self.assertIn("StartDate", record)
            self.assertIn("EndDate", record)

    def test_s2s_preds_raw(self):
        # Test s2s unaligned preds fields
        data: dict = self.data_get("/ai/s2s/preds?start_date=2024-01-01T12:48:00&end_date=2024-01-05T08:00:00")
        for record in data.values():
            self.assertIn("NSLTplus1", record)
            self.assertIsInstance(record["NSLTplus1"], float)
            self.assertIn("NSLTplus2", record)
            self.assertIsInstance(record["NSLTplus2"], float)
            self.assertIn("NSLTplus3", record)
            self.assertIsInstance(record["NSLTplus3"], float)

    def test_s2s_preds_aligned(self):
        # Test s2s aligned preds fields
        data: dict = self.data_get(
            "/ai/s2s/preds?start_date=2024-01-01T19:00:00&end_date=2024-01-04T04:33:00&aligned=True")
        for record in data.values():
            self.assertIn("NetSystemLoad", record)
            self.assertIsInstance(record["NetSystemLoad"], float)
            self.assertIn("NSLP1ago", record)
            self.assertIsInstance(record["NSLP1ago"], float)
            self.assertIn("NSLP2ago", record)
            self.assertIsInstance(record["NSLP2ago"], float)
            self.assertIn("NSLP3ago", record)
            self.assertIsInstance(record["NSLP3ago"], float)

