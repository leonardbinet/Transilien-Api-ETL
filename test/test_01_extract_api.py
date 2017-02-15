from os import sys, path
import unittest
import logging
import json

from api_transilien_manager.settings import BASE_DIR
from api_transilien_manager.mod_01_extract_api import get_station_ids, xml_to_json_item_list, api_date_to_day_time_corrected

logger = logging.getLogger(__name__)


class TestExtractModuleFunctions(unittest.TestCase):

    def test_get_station_ids(self):
        """
        Test function that is supposed to return station ids.
        """
        for filt in ["all", "responding", "scheduled", "top"]:
            stations = get_station_ids(stations=filt)
            self.assertIsInstance(stations, list)
            self.assertIsInstance(stations[0], str)
            self.assertEqual(len(stations[0]), 7)
            self.assertGreater(len(stations), 10)

        self.assertRaises(ValueError, get_station_ids, stations="doesnt exist")

    def test_api_date_to_day_time_corrected(self):
        """
        Test function that transform dates given by api in usable fields:
        - expected_passage_day : "20120523" format
        - expected_passage_time: "12:55:00" format

        Important: dates between 0 and 3 AM are transformed in +24h time format with day as previous day.
        """

        api_date_1 = "23/05/2012 12:55"
        day_result_1 = api_date_to_day_time_corrected(api_date_1, "day")
        time_result_1 = api_date_to_day_time_corrected(api_date_1, "time")
        self.assertEqual(day_result_1, "20120523")
        self.assertEqual(time_result_1, "12:55:00")

        api_date_2 = "01/07/2016 01:32"
        day_result_2 = api_date_to_day_time_corrected(api_date_2, "day")
        time_result_2 = api_date_to_day_time_corrected(api_date_2, "time")
        self.assertEqual(day_result_2, "20160630")
        self.assertEqual(time_result_2, "25:32:00")

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
        file_path = path.join(BASE_DIR, "test", "files", "api_response.xml")
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
