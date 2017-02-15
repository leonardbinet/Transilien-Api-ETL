import os
import pandas as pd
import logging
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer


from api_transilien_manager.utils_rdb import rdb_connection
from api_transilien_manager.mod_01_extract_schedule import build_stop_times_ext_df
from api_transilien_manager.utils_dynamo import dynamo_get_table, dynamo_get_client
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


def dynamo_get_schedule_info(day_train_num, station, full_resp=False):
    """
    Query dynamo to find trip_id and scheduled_departure_time from train_num, station, and day.

    Day is here to double check, avoid errors: yyyymmdd_format.
    Station_id is in 7 digits format.

    Reminder: hash key: station_id
    sort key: day_train_num
    """
    table_name = dynamo_sched_dep

    # Query
    # table = dynamo_get_table(table_name)
    # response1 = table.query(
    #    ConsistentRead=False,
    #    KeyConditionExpression=Key('station_id').eq(
    #        str(station)) & Key('day_train_num').eq(str(day_train_num))
    #)

    # GetItem
    client = dynamo_get_client()
    response = client.get_item(
        TableName=table_name,
        Key={
            "station_id": {
                "S": str(station)
            },
            "day_train_num": {
                "S": str(day_train_num)
            }
        },
        ConsistentRead=False
    )
    if full_resp:
        return response

    # Use a deserializer to parse
    # Return object with ids and trip_id and scheduled_departure_time
    deser = TypeDeserializer()

    response_parsed = {}
    for key, value in response["Item"].items():
        response_parsed[key] = deser.deserialize(value)

    return response_parsed


def dynamo_extend_dataframe_with_schedule(df, full=False):
    # Extract items primary keys and format it for getitem
    extract = df[["day_train_num", "station_id"]]
    extract.station_id = extract.station_id.apply(str)
    # Serialize in dynamo types
    seres = TypeSerializer()
    extract_ser = extract.applymap(seres.serialize)
    items = extract_ser.to_dict(orient="records")

    # Compute query in batches of 100 items
    batches = [items[i:i + 100] for i in range(0, len(items), 100)]

    client = dynamo_get_client()

    responses = []
    unprocessed_keys = []
    for batch in batches:
        response = client.batch_get_item(
            RequestItems={
                dynamo_sched_dep: {
                    'Keys': batch
                }
            }
        )
        responses += response["Responses"][dynamo_sched_dep]
        unprocessed_keys.append(response["UnprocessedKeys"])

    # TODO
