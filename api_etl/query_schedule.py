"""
Module used to query schedule data contained in Dynamo, Mongo or Postgres databases.
"""

import pandas as pd
import json
import logging
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer


from api_etl.utils_rdb import rdb_connection
from api_etl.utils_misc import compute_delay
from api_etl.utils_dynamo import dynamo_get_table, dynamo_get_client, dynamo_submit_batch_getitem_request
from api_etl.settings import dynamo_sched_dep

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


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


# NOT USED IN PRODUCTION

def trip_scheduled_departure_time(trip_id, station):
    """
    DEPRECATED
    """
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
        logger.debug("Found departure time: %s", departure_time)
        return departure_time
    else:
        logger.warning("Multiple scheduled time found: %d matches",
                       len(departure_time))
        return False
    return departure_time
