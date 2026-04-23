import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utilities import utils
from src.utilities import DISCOVERED_UTILITY_MODULES

class TestHelper(unittest.TestCase):

    def test_utility_package_discovery_lists_logger_and_utils(self):
        self.assertIn("src.utilities.logger", DISCOVERED_UTILITY_MODULES)
        self.assertIn("src.utilities.stock_prices", DISCOVERED_UTILITY_MODULES)
        self.assertIn("src.utilities.utils", DISCOVERED_UTILITY_MODULES)

    def test_generateURL(self):
        docID = "test_doc_id"
        expected_url = f"http://test.com/{docID}?type=5&Subscription-Key=test_key"
        self.assertEqual(
            utils.generateURL(docID, "http://test.com", "test_key", "5"),
            expected_url,
        )

    def test_generateURL_default_doctype(self):
        docID = "test_doc_id"
        expected_url = f"http://test.com/{docID}?type=5&Subscription-Key=test_key"
        self.assertEqual(
            utils.generateURL(docID, "http://test.com", "test_key"),
            expected_url,
        )

    def test_json_list_to_csv(self):
        json_list = [
            {"name": "John", "age": 30, "city": "New York"},
            {"name": "Jane", "age": 25, "city": "London"}
        ]
        csv_filename = "test.csv"
        utils.json_list_to_csv(json_list, csv_filename)

        self.assertTrue(os.path.exists(csv_filename))

        with open(csv_filename, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertEqual(header, ["name", "age", "city"])
            row1 = next(reader)
            self.assertEqual(row1, ["John", "30", "New York"])
            row2 = next(reader)
            self.assertEqual(row2, ["Jane", "25", "London"])

        os.remove(csv_filename)

if __name__ == '__main__':
    unittest.main()
