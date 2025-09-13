import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import utils
from config import Config

class TestHelper(unittest.TestCase):

    @patch('config.Config')
    def test_generateURL(self, mock_config):
        # Mock the config
        mock_config_instance = mock_config()
        def get_side_effect(key, default=None):
            if key == "API_KEY":
                return "test_key"
            return {
                "baseURL": "http://test.com",
                "doctype": "5"
            }.get(key, default)
        mock_config_instance.get.side_effect = get_side_effect
        
        docID = "test_doc_id"
        expected_url = f"http://test.com/{docID}?type=5&Subscription-Key=test_key"
        self.assertEqual(utils.generateURL(docID, mock_config_instance), expected_url)

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
