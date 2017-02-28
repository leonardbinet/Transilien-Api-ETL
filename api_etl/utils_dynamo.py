"""
Module used to interact with Dynamo databases.
"""

import logging
import boto3
import pandas as pd
from boto3.dynamodb.types import TypeDeserializer  # TypeSerializer

from api_etl.utils_secrets import get_secret

logger = logging.getLogger(__name__)

AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)

dynamodb = boto3.resource('dynamodb')


def dynamo_get_client():
    return boto3.client("dynamodb")


def dynamo_create_real_departures_table(table_name, read=5, write=5):

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'day_station',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'expected_passage_day',
                'KeyType': 'RANGE'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    logger.info("Table %s created in DynamoDB" % table_name)


def dynamo_get_table_provisionned_capacity(table_name):
    table = dynamodb.Table(table_name)

    provisioned_throughput = table.provisioned_throughput

    read = provisioned_throughput["ReadCapacityUnits"]
    write = provisioned_throughput["WriteCapacityUnits"]
    return read, write


def dynamo_update_provisionned_capacity(read, write, table_name):
    table = dynamodb.Table(table_name)

    table = table.update(
        ProvisionedThroughput={
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write
        }
    )


def dynamo_get_table(table_name):
    return dynamodb.Table(table_name)


def dynamo_insert_batches(items_list, table_name):
    # transform list in batches of 25 elements (max authorized by dynamo API)
    # batches = [items_list[i:i + 25] for i in range(0, len(items_list), 25)]

    table = dynamodb.Table(table_name)

    # write in batches
    # logger.info("Begin writing batches in dynamodb")
    with table.batch_writer() as batch:
        for item in items_list:
            batch.put_item(
                Item=item
            )
    # logger.info("Task completed.")


def dynamo_submit_batch_getitem_request(items_keys, table_name, max_retry=3, prev_resp=None):
    # Compute query in batches of 100 items
    batches = [items_keys[i:i + 100]
               for i in range(0, len(items_keys), 100)]

    client = dynamo_get_client()

    responses = []
    unprocessed_keys = []
    for batch in batches:
        response = client.batch_get_item(
            RequestItems={
                table_name: {
                    'Keys': batch
                }
            }
        )
        try:
            responses += response["Responses"][table_name]
        except KeyError:
            pass
        try:
            unprocessed_keys += response[
                "UnprocessedKeys"][table_name]
        except KeyError:
            pass

    # TODO: add timer
    if len(unprocessed_keys) > 0:
        if max_retry == 0:
            return responses
        else:
            max_retry = max_retry - 1
            return dynamo_submit_batch_getitem_request(unprocessed_keys, table_name, max_retry=max_retry, prev_resp=responses)

    return responses


def dynamo_provisionned_capacity_manager(table_name=None):
    """
    This function will be called every hour.
    We can go up as much as we want, but can go down only 4 times a day.

    For up:
    - go incremental, every hour, given a function

    For down:
    - define four "hour" steps: choose only update hours
    - apply function on these times slots
    """
    pass

    # define prediction function

    # define majoration function: how much margin we want (for the whole next
    # hour)

    # every hour:
    # if func now is higher than the hour before: update higher with
    # majoration function

    # if func now is lower than the hour before: check if hour in update hours
    # if so, set with majoration function


def dynamo_extract_all_table(table_name, max_req=100):
    """
    Returns a dataframe.
    """
    table = dynamodb.Table(table_name)
    response = table.scan()
    data = response['Items']

    while response.get('LastEvaluatedKey') and max_req > 0:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
        max_req -= 1

    resp_df = pd.DataFrame(data)
    return resp_df
