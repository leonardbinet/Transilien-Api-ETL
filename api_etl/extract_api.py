"""
Core module using other modules to:
- extract data from the Transilien's API (use utils_api_client)
- update it with GTFS data (use query_schedule, if data was previously saved
with extract_schedule)
- save it in databases, Mongo or/and Dynamo (use utils_dynamo, utils_mongo)
"""

import copy
import logging
import time
from datetime import datetime
import json

import xmltodict
import pandas as pd

if __name__ == '__main__':
    import logging.config
    logging.config.fileConfig('logging.conf')

from api_etl.utils_misc import (
    get_paris_local_datetime_now, DateConverter, StationProvider
)
from api_etl.utils_api_client import ApiClient
from api_etl.utils_mongo import (
    mongo_async_save_items, mongo_async_upsert_items
)
from api_etl.models import RealTimeDeparture
from api_etl.settings import mongo_realtime_unique, mongo_realtime_all

# To avoid some pandas warnings
pd.options.mode.chained_assignment = None


class ApiExtractor():
    """ Made for unique usage
    """

    def __init__(self, stations, max_per_minute=350):

        assert isinstance(stations, list)
        stations = list(map(str, stations))
        for station in stations:
            assert len(station) == 8
        self.stations = stations

        self.max_per_minute = max_per_minute
        self.request_paris_time = None
        self.raw_responses = []
        self.json_objects = []
        self.dict_objects = []
        self.dynamo_objects = []
        self.saved_dynamo = False
        self.saved_mongo = False

    def request_api_for_stations(self):
        """
        This function will query transilien's API asking for expected trains
        passages in near future on stations defined as paramater, and parse
        responses so that it can be saved in databases.

        Note that stations id required by api are in 8 digits format.

        Note also that the maximum queries per minute accepted by the API is
        350.

        This function will save responses in "raw_responses" attribute, each
        response being a tuple (string response, station).
        """

        logging.info("Extraction of %d stations" % len(self.stations))
        client = ApiClient()
        self.raw_responses = client.request_stations(self.stations)

        # Save at what time the request was made (Paris Time)
        self.request_paris_time = get_paris_local_datetime_now()\
            .replace(tzinfo=None)

        # Parse responses
        self._parse_responses()

    def _parse_responses(self):
        """
        This function parses responses located in "raw_responses" attribute,
        and saves parsed responses in following attributes:
        - json_objects
        - dict_objects
        - dynamo_objects
        """
        # Empties previous requests
        self.json_objects = []
        self.dict_objects = []
        self.dynamo_objects = []
        # Parse responses in JSON format
        logging.info("Parsing")
        for response in self.raw_responses:
            try:
                xml_string = response[0]
                station = response[1]
                self._parse_response(xml_string, station)
            except Exception as e:
                logging.debug("Cannot parse station %s: %s" %
                              (response[1], e))
                continue

    def _parse_response(self, xml_string, station, return_df=False):
        """
        This function transforms transilien's API XML answers into a list of
        objects, in a valid format, and saves it in attributes:
        \n- json_objects
        \n- dict_objects
        \n- dynamo_objects

        :param xml_string: XML string you want to transform
        :type xml_string: string

        :param station: station id that was queried. This is necessary to add a
        station field in output. This must be a 8 digits string or integer. You
        can find the stations ids on SNCF website.
        https://ressources.data.sncf.com/explore/dataset/referentiel-gares-voya
        eurs/
        :type station: string/integer

        """

        # Save with Paris timezone (if server is abroad)
        request_dt = self.request_paris_time

        # Parse XML into a dataframe
        mydict = xmltodict.parse(xml_string)
        trains = mydict["passages"]["train"]
        df_trains = pd.DataFrame(trains)

        # Add custom fields
        df_trains.loc[:, "date"] = df_trains.date.apply(lambda x: x["#text"])
        df_trains.loc[:, "dt_conv"] = df_trains["date"]\
            .apply(lambda x: DateConverter(api_date=x))

        df_trains.loc[:, "expected_passage_day"] = df_trains["dt_conv"]\
            .apply(lambda x: x.special_date)
        df_trains.loc[:, "expected_passage_time"] = df_trains["dt_conv"]\
            .apply(lambda x: x.special_time)

        df_trains.loc[:, "request_day"] = request_dt.strftime('%Y%m%d')
        df_trains.loc[:, "request_time"] = request_dt.strftime('%H:%M:%S')
        df_trains.loc[:, "station_8d"] = str(station)
        df_trains.loc[:, "station_id"] = str(station)[:-1]

        df_trains.rename(columns={'num': 'train_num'}, inplace=True)

        # Data freshness is time in seconds between request time and
        # expected_passage_time: lower is better
        df_trains.loc[:, "data_freshness"] = df_trains\
            .apply(
                lambda x: int(x["dt_conv"].compute_delay_from(
                    dt=request_dt
                )),
            axis=1
        )
        # Delete datetime converter objects
        del df_trains["dt_conv"]

        # Hash key for dynamodb: formerly: day_station, now, station (7 digits)
        # Sort key for dynamodb: formerly: train_num, now, day_train_num
        df_trains.loc[:, "day_train_num"] = df_trains\
            .apply(
                lambda x: "%s_%s" % (
                    x["expected_passage_day"], x["train_num"]),
                axis=1
        )

        # Save only strings in databases
        df_trains = df_trains.applymap(str)

        # Drop possible duplicates on keys:
        df_trains.drop_duplicates(
            subset=["day_train_num", "station_id"],
            inplace=True
        )

        jsons_to_add = json.loads(df_trains.to_json(orient='records'))
        self.json_objects.extend(jsons_to_add)

        dicts_to_add = df_trains.to_dict(orient='records')
        self.dict_objects.extend(dicts_to_add)

        dynamo_objects_to_add = [
            RealTimeDeparture(**item) for item in dicts_to_add
        ]

        self.dynamo_objects.extend(dynamo_objects_to_add)

        if return_df:
            return df_trains

    def save_in_dynamo(self):
        """
        Saves objects in dynamo database.
        """
        logging.info("Upsert of %d objects in dynamo",
                     len(self.dynamo_objects))
        with RealTimeDeparture.batch_write() as batch:
            for obj in self.dynamo_objects:
                batch.save(obj)
        # Previously:
        # dynamo_insert_batches(items_list, table_name = dynamo_real_dep)

    def save_in_mongo(self, mongo_unique, mongo_all):
        """
        This function will save items in Mongo.

        :param mongo_unique: save items in dynamo table that save unique
        passages
        :type mongo_unique: boolean

        :param mongo_all: save items in dynamo table that save all passages
        :type mongo_all: boolean
        """
        assert (mongo_unique or mongo_all)

        # Make deep copies, because mongo will add _ids
        items_list2 = copy.deepcopy(self.json_objects)
        items_list3 = copy.deepcopy(self.json_objects)

        if mongo_all:
            # Save items in collection without compound primary key
            logging.info("Saving  %d items in Mongo departures collection",
                         len(items_list2))
            mongo_async_save_items(mongo_realtime_all["name"], items_list2)

        if mongo_unique:
            # Save items in collection with compound primary key
            index_fields = mongo_realtime_unique["unique_index"]
            logging.info(
                "Upsert of %d items of json data in Mongo %s collection",
                len(items_list3),
                mongo_realtime_unique["name"]
            )
            mongo_async_upsert_items(
                mongo_realtime_unique["name"], items_list3, index_fields)


def operate_one_cycle(
    station_filter=False, dynamo_unique=True, mongo_unique=False,
    mongo_all=False
):
    """
    This function performs the extract_save_stations operation in two steps.
    First half of stations, then second half of stations. This is made so that
    we do not exceed transilien's api max requests per minute (350/min).

    :param station_filter: default to False. If no filter, the function will
    take all stations provided by the StationProvider class. If set to list,
    these are the stations for which the transilien's api will be queried. List
    of stations of length 8.
    :type station_filter: list of str/int OR False

    :param dynamo_unique: save items in dynamo table that save unique passages
    :type dynamo_unique: boolean

    :param mongo_unique: save items in dynamo table that save unique passages
    :type mongo_unique: boolean

    :param mongo_all: save items in dynamo table that save all passages
    :type mongo_all: boolean
    """
    if not station_filter:
        station_list = StationProvider().get_stations_per_line()
    else:
        station_list = station_filter

    # station_chunks = chunks(station_list, max_per_minute)
    # split stations in two of same size
    station_chunks = [station_list[i::2] for i in range(2)]

    for i, station_chunk in enumerate(station_chunks):
        chunk_begin_time = datetime.now()

        extractor = ApiExtractor(station_chunk)
        extractor.request_api_for_stations()

        if dynamo_unique:
            extractor.save_in_dynamo()

        if mongo_unique or mongo_all:
            extractor.save_in_mongo(
                mongo_unique=mongo_unique,
                mongo_all=mongo_all
            )

        time_passed = (datetime.now() - chunk_begin_time).seconds
        logging.info("Time spent: %d seconds", int(time_passed))

        # Max per minute: so have to wait
        if time_passed > 60:
            logging.warning(
                "Chunk time took more than one minute: %d seconds",
                time_passed
            )

        if time_passed < 60 and i != 1:
            # doesn't wait on last cycle (0,1 -> last cycle is 1)
            time.sleep(60 - time_passed)


def operate_multiple_cycles(
    station_filter=False, cycle_time_sec=1200, stop_time_sec=3600
):
    """
    Deprecated: a cron job is scheduled to run operate_one_cycle every 'n'
    minutes.

    This function performs the operate_one_cycle operation every
    'cycle_time_sec' seconds (default 10 minutes), during the time defined as
    'stop_time_sec' (default 60 minutes).

    :param station_filter: default to False. If no filter, the function will
    take all stations provided by the get_station_ids function. If set to list,
    these are the stations for which the transilien's api will be queried. List
    of stations of length 8.
    :type station_filter: list of str/int OR False

    :param cycle_time_sec: number of seconds between each cycle beginning.
    :type cycle_time_sec: int

    :param stop_time_sec: number of seconds until a new cycle cannot begin.
    :type stop_time_sec: int
    """

    logging.info(
        "BEGINNING OPERATION WITH LIMIT OF %d SECONDS",
        stop_time_sec
    )
    begin_time = datetime.now()

    while (datetime.now() - begin_time).seconds < stop_time_sec:
        # Set cycle loop
        loop_begin_time = datetime.now()
        logging.info("BEGINNING CYCLE OF %d SECONDS", cycle_time_sec)

        operate_one_cycle(station_filter=station_filter)

        # Wait until beginning of next cycle
        time_passed = (datetime.now() - loop_begin_time).seconds
        logging.info("Time spent on cycle: %d seconds", int(time_passed))
        if time_passed < cycle_time_sec:
            time_to_wait = cycle_time_sec - time_passed
            logging.info("Waiting %d seconds till next cycle.", time_to_wait)
            time.sleep(time_to_wait)
        else:
            logging.warning(
                "Cycle time took more than expected: %d seconds", time_passed)

        # Information about general timing
        time_from_begin = (datetime.now() - begin_time).seconds
        logging.info(
            "Time spent from beginning: %d seconds. (stop at %d seconds)",
            time_from_begin, stop_time_sec
        )
