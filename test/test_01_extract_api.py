from os import sys, path
import unittest
from src.settings import BASE_DIR
import logging

from src.mod_01_extract_api import get_station_ids, xml_to_json_item_list, api_date_to_day_time_corrected

logger = logging.getLogger(__name__)


class TestExtractModuleFunctions(unittest.TestCase):

    def test_get_station_ids(self):
        UIC_stations = get_station_ids()
        UIC7_stations = get_station_ids(id_format="UIC7")
        self.assertIsInstance(UIC_stations, list)
        self.assertIsInstance(UIC_stations[0], str)
        self.assertEqual(len(UIC_stations[0]), 8)
        self.assertEqual(len(UIC7_stations[0]), 7)
        self.assertEqual(len(UIC_stations), len(UIC7_stations))
        self.assertRaises(ValueError, get_station_ids, id_format="notUIC")

    def test_api_date_to_day_time_corrected(self):
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
        file_path = path.join(BASE_DIR, "test", "files", "api_response.xml")
        with open(file_path, 'r') as xml_file:
            xml_string = xml_file.read()
        json_list = xml_to_json_item_list(xml_string, "87393009")

        # Test if a list of dict (json serializable) at the end
        self.assertIsInstance(json_list, list)
        self.assertIsInstance(json_list[0], dict)

        # Test that we find all necessary fields
        necessary_fields = ["date", "request_day",
                            "request_time", "train_num", "miss", "station", "expected_passage_day", "expected_passage_time"]
        json_keys_list = list(map(lambda x: list(x.keys()), json_list))
        for json_item_keys in json_keys_list:
            keys_all_exist = all(
                key in json_item_keys for key in necessary_fields)
            self.assertTrue(keys_all_exist)

        # logger.debug(json_list)


if __name__ == '__main__':
    unittest.main()
