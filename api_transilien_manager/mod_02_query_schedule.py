import os
import pandas as pd
import json
import logging
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer


from api_transilien_manager.utils_rdb import rdb_connection
from api_transilien_manager.utils_misc import compute_delay
from api_transilien_manager.utils_dynamo import dynamo_get_table, dynamo_get_client, dynamo_submit_batch_getitem_request
from api_transilien_manager.settings import dynamo_sched_dep

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


def dynamo_extend_items_with_schedule(items_list, full=False, df_format=False):
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
    logger.info("Asked to find schedule and trip_id for %d items, we found %d of them." % (
        len(df), len(resp_df)))
    if df_format:
        return df_updated

    # Safe json serializable python dict
    df_updated = df_updated.applymap(str)
    items_updated = json.loads(df_updated.to_json(orient='records'))
    return items_updated


# NOT USED IN PRODUCTION

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
