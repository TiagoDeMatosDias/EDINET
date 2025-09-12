
import unittest
import os
import csv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import os
import csv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import utils
from config import Config

class TestHelper(unittest.TestCase):

    def test_generateURL(self):
        config = Config()
        docID = "test_doc_id"
        expected_url = f"{config.get('baseURL')}/{docID}?type={config.get('doctype')}&Subscription-Key={config.get('apikey')}"
        self.assertEqual(utils.generateURL(docID, config), expected_url)

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

from config import Config

class TestHelper(unittest.TestCase):

    def test_generateURL(self):
        config = Config()
        docID = "test_doc_id"
        expected_url = f"{config.get('baseURL')}/{docID}?type={config.get('doctype')}&Subscription-Key={config.get('apikey')}"
        self.assertEqual(utils.generateURL(docID, config), expected_url)

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
