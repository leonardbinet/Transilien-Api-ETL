"""
Module used to interact with Dynamo databases.
"""

import logging
import boto3
import pandas as pd

from api_etl.utils_secrets import get_secret

# Set as environment variable: boto takes it directly
AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)

logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb')


def dynamo_get_client():
    """
    Return Dynamo client (credentials already set up)
    """
    return boto3.client("dynamodb")


def dynamo_create_real_departures_table(
    table_name, read=5, write=5,
    hash_key="day_station", range_key="expected_passage_day"
):
    """
    Creates a table in Dynamo with given hash and range keys, with provisioned
    throughput given in parameters.

    :param table_name: table name
    :type table_name: str

    :param read: table read provisioned throughput
    :type read: int

    :param write: table write provisioned throughput
    :type write: int

    :param hash_key: table Hash Key
    :type hash_key: str

    :param range_key: table Range Key
    :type range_key: str
    """

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': hash_key,
                'KeyType': 'HASH'
            },
            {
                'AttributeName': range_key,
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
    logger.info("Table %s created in DynamoDB", table_name)


def dynamo_get_table_provisionned_capacity(table_name):
    """
    This function returns a given table provisioned throughput. Returns (read,
    write)

    :param table_name: table name
    :type table_name: str
    """

    table = dynamodb.Table(table_name)

    provisioned_throughput = table.provisioned_throughput

    read = provisioned_throughput["ReadCapacityUnits"]
    write = provisioned_throughput["WriteCapacityUnits"]
    return read, write


def dynamo_update_provisionned_capacity(read, write, table_name):
    """
    Update a table provisioned throughput.

    :param table_name: table name
    :type table_name: str

    :param read: table read provisioned throughput
    :type read: int

    :param write: table write provisioned throughput
    :type write: int
    """
    table = dynamodb.Table(table_name)

    table = table.update(
        ProvisionedThroughput={
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write
        }
    )


def dynamo_get_table(table_name):
    """
    Get a Dynamo table object.

    :param table_name: table name
    :type table_name: str
    """
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


def dynamo_submit_batch_getitem_request(
    items_keys, table_name, max_retry=3, prev_resp=None
):
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


def dynamo_extract_whole_table(table_name, max_req=100):
    """
    Extract content of a dynamo table. With limit of "max_req" requests.
    Returns a dataframe.

    :param table_name: table name
    :type table_name: str

    :param max_req: maximum number of requests
    :type max_req: int

    :rtype: pandas dataframe
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
