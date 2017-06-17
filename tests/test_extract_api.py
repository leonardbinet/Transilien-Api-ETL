"""
Tests for extract_api module.
"""

from os import path
import unittest
import logging
import json

from api_etl.settings import __BASE_DIR__
from api_etl.extract_api import xml_to_json_item_list

logger = logging.getLogger(__name__)


class TestExtractModuleFunctions(unittest.TestCase):

    def test_xml_to_json_item_list(self):
        """
        Test XML transformation process into json:
        - loads a XML sample file
        - check that it returns a list of elements
        - check that each element is a dict
        - check that each element is json serializable
        - check that each element contains necessary_fields
        """

        # Open a xml file that is supposed to be from station 87393009
        file_path = path.join(__BASE_DIR__, "tests", "files", "api_response.xml")
        with open(file_path, 'r') as xml_file:
            xml_string = xml_file.read()
        output = xml_to_json_item_list(xml_string, "87393009")

        necessary_fields = ["date", "request_day",
                            "request_time", "train_num", "miss", "station_id", "expected_passage_day", "expected_passage_time", "day_train_num"]

        def is_jsonable(x):
            try:
                json.dumps(x)
                return True
            except:
                return False

        # Begin test
        self.assertIsInstance(output, list)

        for element in output:
            # Check that each element is a dict
            self.assertIsInstance(element, dict)
            # With all necessary fields
            element_keys_list = list(element.keys())
            keys_all_exist = all(
                key in element_keys_list for key in necessary_fields)
            self.assertTrue(keys_all_exist)
            # That we can transform into json
            self.assertTrue(is_jsonable(element))


if __name__ == '__main__':
    unittest.main()
