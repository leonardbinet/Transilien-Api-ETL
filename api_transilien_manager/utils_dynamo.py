from os import sys, path
import logging
import boto3
import time
from datetime import datetime

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


def dynamo_create_real_departures_table(table_name=None, read=5, write=5):
    if not table_name:
        table_name = dynamo_table

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


def dynamo_get_table_provisionned_capacity(table_name=None):
    if not table_name:
        table_name = dynamo_table
    table = dynamodb.Table(table_name)

    provisioned_throughput = table.provisioned_throughput

    read = provisioned_throughput["ReadCapacityUnits"]
    write = provisioned_throughput["WriteCapacityUnits"]
    return read, write


def dynamo_update_provisionned_capacity(read, write, table_name=None):
    if not table_name:
        table_name = dynamo_table

    table = dynamodb.Table(table_name)

    table = table.update(
        ProvisionedThroughput={
            'ReadCapacityUnits': read,
            'WriteCapacityUnits': write
        }
    )


def dynamo_insert_batches(items_list, table_name=None):
    # transform list in batches of 25 elements (max authorized by dynamo API)
    # batches = [items_list[i:i + 25] for i in range(0, len(items_list), 25)]

    # choose table name
    if not table_name:
        table_name = dynamo_table

    table = dynamodb.Table(table_name)

    # write in batches
    # logger.info("Begin writing batches in dynamodb")
    with table.batch_writer() as batch:
        for item in items_list:
            batch.put_item(
                Item=item
            )
    # logger.info("Task completed.")


def dynamo_spread_writes_over_minute(items_list, table_name=None, splits=6):
    # choose table name
    if not table_name:
        table_name = dynamo_table

    # split in 'splits' parts
    batches = [items_list[i::splits] for i in range(splits)]

    # time to wait between operations: spread over one minute
    cycle_time_sec = float(60) / splits

    logger.info("Dynamo: saving data for over next minute in %d steps." % splits)

    for i, batch in enumerate(batches):
        loop_begin_time = datetime.now()

        try:
            dynamo_insert_batches(batch, table_name=table_name)
        except Exception as e:
            logger.error("Error on batch number %d, error: %s" %
                         (i, e.with_traceback()))

        time_passed = (datetime.now() - loop_begin_time).seconds
        if i != (len(batches) - 1):
            # don't wait on last round
            if time_passed < cycle_time_sec:
                time_to_wait = cycle_time_sec - time_passed
                logger.debug("Waiting %d seconds till next cycle." %
                             time_to_wait)
                time.sleep(time_to_wait)
    logger.info("Operation completed")


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
    if not table_name:
        table_name = dynamo_table

    # define prediction function

    # define majoration function: how much margin we want (for the whole next
    # hour)

    # every hour:
    # if func now is higher than the hour before: update higher with
    # majoration function

    # if func now is lower than the hour before: check if hour in update hours
    # if so, set with majoration function
