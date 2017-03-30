"""
Module used to query schedule data contained in Dynamo, Mongo or Postgres databases.
"""

import pandas as pd
import json
import logging
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

from api_etl.utils_misc import compute_delay, get_paris_local_datetime_now
from api_etl.utils_dynamo import dynamo_submit_batch_getitem_request
from api_etl.utils_rdb import Provider, ResultSerializer
from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route
)
from api_etl.settings import dynamo_sched_dep

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


class RdbQuerier():

    def __init__(self):
        self.provider = Provider()

    def services_of_day(self, yyyymmdd=None):
        if not yyyymmdd:
            yyyymmdd = get_paris_local_datetime_now().strftime("%Y%m%d")

        yyyymmdd = str(yyyymmdd)
        assert len(yyyymmdd) == 8
        all_services = self.provider.get_session()\
            .query(Calendar.service_id)\
            .filter(Calendar.start_date <= yyyymmdd)\
            .filter(Calendar.end_date >= yyyymmdd)\
            .all()

        # Get service exceptions
        # 1 = service (instead of usually not)
        # 2 = no service (instead of usually yes)

        serv_add = self.provider.get_session()\
            .query(CalendarDate.service_id)\
            .filter(CalendarDate.date == yyyymmdd)\
            .filter(CalendarDate.exception_type == "1")\
            .all()

        serv_rem = self.provider.get_session()\
            .query(CalendarDate.service_id)\
            .filter(CalendarDate.date == yyyymmdd)\
            .filter(CalendarDate.exception_type == "2")\
            .all()

        serv_on_day = set(all_services)
        serv_on_day.update(serv_add)
        serv_on_day = serv_on_day - set(serv_rem)
        serv_on_day = map(lambda x: x[0], serv_on_day)
        serv_on_day = list(serv_on_day)

        return serv_on_day

    def trip_stops(self, trip_id):
        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(Trip.trip_id == trip_id)\
            .all()
        return ResultSerializer(results)

    def station_trips_stops(self, station_id, yyyymmdd=None):
        """ station_id should be in 7 digits gtfs format
        """
        station_id = str(station_id)
        assert len(station_id) == 7

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(StopTime.stop_id.match(station_id))\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Calendar.service_id.in_(self.services_of_day(yyyymmdd)))\
            .all()

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(StopTime.stop_id.like("%" + station_id))\
            .filter(Trip.service_id.in_(self.services_of_day(yyyymmdd)))\
            .all()
        #

        return ResultSerializer(results)


def dynamo_extend_items_with_schedule(items_list, full=False, df_format=False):
    """
    This function takes as input 'train stop times' items collected from the transilien's API and extend them with informations from schedule.

    The main goals are:
    - to find for a given 'train stop time' what was the trip_id (transilien's api provide train_num which do not match with trip_id)
    - to find at what time this stop was scheduled (transilien's api provide times at which trains are predicted to arrive at a given time, updated in real-time)

    The steps will be:
    - extract index fields ("day_train_num", "station_id") from input items
    - send queries to Dynamo's 'scheduled_departures' table to find their trip_ids, scheduled_departure_time and other useful informations (line, route, agency etc)
    - extend initial items with information found from schedule

    :param items_list: the items you want to extend. They must be in relevant format, and contain fields that are used as primary fields.
    :type item_list: list of dictionnaries of strings (json serializable)

    :param full: default False. If set to True, items returned will be extended with all fields contained in scheduled_departure table (more detail on trains).
    :type full: boolean

    :param df_format: default False. If set to True, will return a pandas dataframe
    :type df_format: boolean

    :rtype: list of json serializable objects, or pandas dataframe if df_format is set to True.
    """

    df = pd.DataFrame(items_list)
    # Extract items primary keys and format it for getitem
    extract = df[["day_train_num", "station_id"]]
    extract.station_id = extract.station_id.apply(str)

    # Serialize in dynamo types
    seres = TypeSerializer()
    extract_ser = extract.applymap(seres.serialize)
    items_keys = extract_ser.to_dict(orient="records")

    # Submit requests
    responses = dynamo_submit_batch_getitem_request(
        items_keys, dynamo_sched_dep)

    # Deserialize into clean dataframe
    resp_df = pd.DataFrame(responses)
    deser = TypeDeserializer()
    resp_df = resp_df.applymap(deser.deserialize)

    # Select columns to keep:
    all_columns = [
        'arrival_time', 'block_id', 'day_train_num', 'direction_id',
        'drop_off_type', 'pickup_type', 'route_id', 'route_short_name',
        'scheduled_departure_day', 'scheduled_departure_time', 'service_id',
        'station_id', 'stop_headsign', 'stop_id', 'stop_sequence', 'train_num',
        'trip_headsign', 'trip_id'
    ]
    columns_to_keep = [
        'day_train_num', 'station_id',
        'scheduled_departure_time', 'trip_id', 'service_id',
        'route_short_name', 'trip_headsign', 'stop_sequence'
    ]
    if full:
        resp_df = resp_df[all_columns]
    else:
        resp_df = resp_df[columns_to_keep]

    # Merge to add response dataframe to initial dataframe
    # We use left jointure to keep items even if we couldn't find schedule
    index_cols = ["day_train_num", "station_id"]
    df_updated = df.merge(resp_df, on=index_cols, how="left")

    # Compute delay
    df_updated.loc[:, "delay"] = df_updated.apply(lambda x: compute_delay(
        x["scheduled_departure_time"], x["expected_passage_time"]), axis=1)

    # Inform
    logger.info(
        "Asked to find schedule and trip_id for %d items, we found %d of them.",
        len(df), len(resp_df)
    )
    if df_format:
        return df_updated

    # Safe json serializable python dict
    df_updated = df_updated.applymap(str)
    items_updated = json.loads(df_updated.to_json(orient='records'))
    return items_updated
