from os import sys, path
import logging
import boto3

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="utils_dynamo.log")

from api_transilien_manager.utils_secrets import get_secret
from api_transilien_manager.settings import dynamo_table

logger = logging.getLogger(__name__)

AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)


dynamodb = boto3.resource('dynamodb')


def dynamo_create_real_departures_table(name=None):
    if not name:
        name = dynamo_table

    table = dynamodb.create_table(
        TableName=name,
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
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=name)
    logger.info("Table %s created in DynamoDB" % name)


def dynamo_insert_batches(items_list, table_name=None):
    # transform list in batches of 25 elements (max authorized by dynamo API)
    # batches = [items_list[i:i + 25] for i in range(0, len(items_list), 25)]

    # choose table name
    if not table_name:
        table_name = dynamo_table

    table = dynamodb.Table(table_name)

    # write in batches
    logger.info("Begin writing batches in dynamodb")
    with table.batch_writer() as batch:
        for item in items_list:
            batch.put_item(
                Item=item
            )
    logger.info("Task completed.")
