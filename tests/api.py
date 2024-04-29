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
        keys: list = list(data.keys())
        first, last = data[keys[0]], data[keys[-1]]

        for record in [first, last]:
            self.assertIn("Latitude", record)
            self.assertIn("Longitude", record)
            self.assertIn("Elevation", record)
            self.assertIn("StationName", record)
            self.assertIn("RegioName", record)

    def test_omsz_status(self):
        # Test omsz status fields
        data: dict = self.data_get("/omsz/status")
        keys: list = list(data.keys())
        first, last = data[keys[0]], data[keys[-1]]

        for record in [first, last]:
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

    def test_mavir_logo(self):
        # Test if mavir logo is valid and available
        response: Response = self.client_get("/mavir/logo")
        logo_response: Response = httpx.get(response.json())
        self.assertEqual(logo_response.status_code, 200)

    def test_mavir_status(self):
        # Test mavir status fields
        data: dict = self.data_get("/mavir/status")
        keys: list = list(data.keys())
        first, last = data[keys[0]], data[keys[-1]]

        for record in [first, last]:
            self.assertIn("StartDate", record)
            self.assertIn("EndDate", record)

    def test_mavir_columns(self):
        # Test mavir columns response
        data: dict = self.data_get("/mavir/columns")
        # Testing if it's dict to check that it provides measurement units too
        self.assertIsInstance(data, dict)

    def test_ai_columns(self):
        # Test ai columns response
        data: dict = self.data_get("/ai/columns")
        # Testing if it's dict to check that it provides measurement units too
        self.assertIsInstance(data, dict)

    def test_s2s_status(self):
        # Test s2s status fields
        data: dict = self.data_get("/ai/s2s/status")
        keys: list = list(data.keys())
        first, last = data[keys[0]], data[keys[-1]]  # Currently first == last

        for record in [first, last]:
            self.assertIn("StartDate", record)
            self.assertIn("EndDate", record)

    def test_s2s_preds_raw(self):
        # Test s2s unaligned preds fields
        data: dict = self.data_get("/ai/s2s/preds?start_date=2024-01-01T12:00:00&end_date=2024-01-05T08:00:00")
        keys: list = list(data.keys())
        first, middle, last = data[keys[0]], data[keys[len(data) // 2]], data[keys[-1]]

        for record in [first, middle, last]:
            self.assertIn("NSLTplus1", record)
            self.assertIsInstance(record["NSLTplus1"], float)
            self.assertIn("NSLTplus2", record)
            self.assertIsInstance(record["NSLTplus2"], float)
            self.assertIn("NSLTplus3", record)
            self.assertIsInstance(record["NSLTplus3"], float)

