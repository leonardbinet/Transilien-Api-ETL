import os
import pandas as pd
import logging
from boto3.dynamodb.conditions import Key, Attr

from api_transilien_manager.utils_rdb import rdb_connection
from api_transilien_manager.mod_01_extract_schedule import build_stop_times_ext_df
from api_transilien_manager.utils_dynamo import dynamo_get_table
from api_transilien_manager.settings import dynamo_sched_dep

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


def trip_scheduled_departure_time(trip_id, station):
    # Check parameters
    if len(str(station)) == 8:
        station = str(station)[:-1]
    elif len(str(station)) == 7:
        station = str(station)
    else:
        logger.error("Station must be 7 digits (8 accepted)")
        return False

    # Make query
    connection = rdb_connection()
    cursor = connection.cursor()
    query = "SELECT departure_time FROM stop_times_ext WHERE trip_id='%s' AND station_id='%s';" % (
        trip_id, station)
    cursor.execute(query)
    departure_time = cursor.fetchone()
    connection.close()

    # Check number of results
    if not departure_time:
        logger.warning("No matching scheduled_departure_time")
        return False
    elif len(departure_time) == 0:
        logger.warning("No matching scheduled_departure_time")
        return False
    elif len(departure_time) == 1:
        departure_time = departure_time[0]
        logger.debug("Found departure time: %s" % departure_time)
        return departure_time
    else:
        logger.warning("Multiple scheduled time found: %d matches" %
                       len(departure_time))
        return False
    return departure_time


def dynamo_get_trip_id_and_sch_dep_time(train_num, station, day):
    """
    Query dynamo to find trip_id and scheduled_departure_time from train_num, station, and day.

    Day is here to double check, avoid errors: yyyymmdd_format.
    Station_id is in 7 digits format.

    Reminder: hash key: station_id
    sort key: day_train_num
    """
    day_train_num = "%s_%s" % (day, train_num)

    table = dynamo_get_table(dynamo_sched_dep)
    response = table.query(
        Select="ALL_ATTRIBUTES",
        ConsistentRead=False,
        KeyConditionExpression=Key('station_id').eq(station),
        FilterExpression=Attr('day_train_num').eq(day_train_num)
    )
    if not response:
        return None

    try:
        scheduled_departure_time = response["scheduled_departure_time"]
        trip_id = response["trip_id"]
    except KeyError as e:
        logger.debug("Keys not found in response: %s" % e)


"""

def save_scheduled_departures_of_day_mongo(yyyymmdd_format):
    json_list = get_departure_times_of_day_json_list(yyyymmdd_format)

    index_fields = ["scheduled_departure_day", "station_id", "train_num"]

    logger.info(
        "Upsert of %d items of json data in Mongo scheduled_departures collection" % len(json_list))

    mongo_async_upsert_items("scheduled_departures", json_list, index_fields)


def rdb_get_departure_times_of_day_json_list(yyyymmdd_format):
    # Check passed parameters
    try:
        int(yyyymmdd_format)
    except Exception as e:
        logger.error("Date provided is not valid %s, %s" %
                     (yyyymmdd_format, e))
        return False
    if len(yyyymmdd_format) != 8:
        logger.error("Date provided is not valid %s" %
                     (yyyymmdd_format))
        return False

    # Find weekday for day condition
    datetime_format = datetime.datetime.strptime(yyyymmdd_format, "%Y%m%d")
    weekday = calendar.day_name[datetime_format.weekday()].lower()

    # Make query
    connection = rdb_connection()
    query = "SELECT * FROM stop_times_ext WHERE start_date<=%s AND end_date>=%s;" % (
        yyyymmdd_format, yyyymmdd_format)
    matching_stop_times = pd.read_sql(query, connection)

    # Rename departure_time column, and add requested date
    matching_stop_times.loc[:, "scheduled_departure_day"] = yyyymmdd_format
    matching_stop_times.rename(
        columns={'departure_time': 'scheduled_departure_time'}, inplace=True)

    json_list = json.loads(matching_stop_times.to_json(orient='records'))
    logger.info("There are %d scheduled departures on %s" %
                (len(json_list), yyyymmdd_format))
    return json_list
"""
