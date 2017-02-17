from os import sys, path
import logging
from boto3.dynamodb.conditions import Key

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="mod_04_queries.log")

from api_transilien_manager.utils_dynamo import dynamo_get_table
from api_transilien_manager.settings import dynamo_sched_dep, dynamo_real_dep
from api_transilien_manager.utils_misc import get_paris_local_datetime_now

logger = logging.getLogger(__name__)


# Request all passages in given station (real and expected, not planned)

def dynamo_get_trains_in_station(station, day=None, max_req=100):
    """
    Query items in real_departures table, for today, in a given station

    Reminder: hash key: station_id
    sort key: day_train_num
    """
    table_name = dynamo_real_dep
    paris_date = get_paris_local_datetime_now()
    paris_date_str = paris_date.strftime("%Y%m%d")

    # Query
    table = dynamo_get_table(table_name)
    response = table.query(
        ConsistentRead=False,
        KeyConditionExpression=Key('station_id').eq(
            str(station)) & Key('day_train_num').begins_with(paris_date_str)
    )
    data = response['Items']

    while response.get('LastEvaluatedKey') and max_req > 0:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
        max_req -= 1
    return data

    """
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


    # Use a deserializer to parse
    # Return object with ids and trip_id and scheduled_departure_time
    deser = TypeDeserializer()

    response_parsed = {}
    for key, value in response["Item"].items():
        response_parsed[key] = deser.deserialize(value)

    return response_parsed
    """
