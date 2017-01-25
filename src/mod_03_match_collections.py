import os
from os import sys, path
import pandas as pd
from datetime import datetime
import calendar
import logging

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="mod_03_match.log")

from src.utils_rdb import sqlite_get_connection
from src.utils_mongo import mongo_get_collection, mongo_async_update_items
from src.mod_02_query_schedule import get_departure_times_of_day_json_list, trip_scheduled_departure_time
from src.settings import BASE_DIR, data_path, gtfs_path

logger = logging.getLogger(__name__)


# trip_id is unique for ONE DAY
# to know exactly the schedule of a train, you need to tell: trip_id AND day
# next, station to get time
def update_real_departures_mongo(yyyymmdd_request_day):
    """
    Update real_departures with scheduled departure times for a given request day:
    - iterate over all elements in real_departures collection for that day
    - find their real id
    - update with information from schedule
    """

    real_departures_col = mongo_get_collection("real_departures")
    real_dep_on_day = list(real_departures_col.find(
        {"request_day": yyyymmdd_request_day}))

    logger.info("Found %d elements in real_departures collection." %
                len(real_dep_on_day))

    items_to_update = []
    for item in real_dep_on_day:
        try:
            item_id = item["_id"]
            train_num = item["num"]
            station = item["station"]
            real_departure_date = item["date"]
            logger.debug("Update train %s on station %s on day %s" %
                         (train_num, station, real_departure_date))
            item_trip_id = api_train_num_to_trip_id(
                train_num, yyyymmdd_request_day)
            if not item_trip_id:
                continue
            scheduled_departure_time = trip_scheduled_departure_time(
                item_trip_id, station)
            if not scheduled_departure_time:
                continue
            delay = compute_delay(
                scheduled_departure_time, real_departure_date)
            item_to_update = (
                {"_id": item_id},
                {"$set":
                 {"scheduled_departure_time": scheduled_departure_time,
                  "trip_id": item_trip_id,
                  "delay": delay
                  }
                 })
            items_to_update.append(item_to_update)
            logger.info("Item prepared successfully")
        except Exception as e:
            logger.warn(
                "Couldn't find trip_id or scheduled departure time for: %s, exception %s" % (item, e))
            continue
    logger.info("Real departures gathering finished. Beginning update.")

    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]
    update_chunks = chunks(items_to_update, 1000)
    for i, update_chunk in enumerate(update_chunks):
        logger.info(
            "Processing chunk number %d of 1000 elements to update." % i)
        mongo_async_update_items("real_departures", update_chunk)


def api_train_num_to_trip_id(train_num, yyyymmdd_day, weekday=None):
    # Check parameters train_num and departure_day

    # Make query
    connection = sqlite_get_connection()
    cursor = connection.cursor()
    query = "SELECT trip_id FROM trips_ext WHERE train_num=? AND start_date<=? AND end_date>=?;"
    cursor.execute(query, (train_num, yyyymmdd_day, yyyymmdd_day))
    trip_ids = cursor.fetchall()

    # Check number of results
    if not trip_ids:
        logger.warning("No matching trip_id")
        connection.close()
        return False
    elif len(trip_ids) == 1:
        trip_id = trip_ids[0][0]
        connection.close()
        logger.debug("Found trip_id: %s" % trip_ids)
        return trip_id
    else:
        logger.warning("Multiple trip_ids found: %d matches: %s" %
                       (len(trip_ids), trip_ids))
        connection.close()
        return False


def get_trip_ids_from_day_and_train_nums(train_num_list, departure_date):
    df_flat = []

    weekday = departure_date_to_week_day(departure_date)
    yyyymmdd_format = departure_date_to_yyyymmdd_date(departure_date)

    # Check weekday, and service beginning and end
    cond1 = df_flat[weekday] == 1
    cond2 = df_flat["start_date"] <= int(yyyymmdd_format)
    cond3 = df_flat["end_date"] >= int(yyyymmdd_format)
    df_poss = df_flat[cond1][cond2][cond3]

    # We keep only asked train nums
    num_trip_id_list = []
    for train_num in train_num_list:
        df_poss = df_poss["train_id"] == int(train_num)
        potential_trip_ids = list(df_poss.trip_id.unique())
        n = len(potential_trip_ids)

        if n == 0:
            logger.warn("No matching trip id for num %s on %s" %
                        (train_num, yyyymmdd_format))
            num_trip_id_list.append((train_num, False))
        elif n == 1:
            logger.info("Trip id found for num %s on %s" %
                        (train_num, yyyymmdd_format))
            num_trip_id_list.append((train_num, potential_trip_ids[0]))
        else:
            logger.warn("Multiple trip ids found: %d matches" % n)
            num_trip_id_list.append((train_num, False))
    return num_trip_id_list


def schedules_from_train_nums_day(train_num_list, yyyymmdd_day):
    pass


def departure_date_to_week_day(departure_date):
    # format: "01/02/2017 22:12"
    departure_date = datetime.strptime(departure_date, "%d/%m/%Y %H:%M")
    weekday = calendar.day_name[departure_date.weekday()]
    return weekday.lower()


def departure_date_to_yyyymmdd_date(departure_date):
    # format: "01/02/2017 22:12" to "20170201"
    departure_date = datetime.strptime(departure_date, "%d/%m/%Y %H:%M")
    new_format = departure_date.strftime("%Y%m%d")
    return new_format


def compute_delay(scheduled_departure_time, real_departure_date):
    # real_departure_date = "01/02/2017 22:12" (api format)
    # scheduled_departure_time = '22:12:00' (schedules format)
    # scheduled_departure_day = '20170102' (schedules format)
    # We don't need to take into account time zones

    real_departure_date = datetime.strptime(
        real_departure_date, "%d/%m/%Y %H:%M")

    scheduled_departure_date = datetime.strptime(
        scheduled_departure_time, "%H:%M:%S")
    # Year and month, same as real
    scheduled_departure_date.replace(year=real_departure_date.year)
    scheduled_departure_date.replace(month=real_departure_date.month)
    scheduled_departure_date.replace(day=real_departure_date.day)
    # For day, might be different (after midnight) => Last two digits
    # scheduled_day = scheduled_departure_day[-2:]
    # scheduled_departure_date.replace(day=scheduled_day)

    # If late: delay is positive, if in advance, it is negative
    delay = real_departure_date - scheduled_departure_date
    return delay.seconds


def api_passage_information_to_delay(num, departure_date, station):
    trip_id = get_trip_ids_from_day_and_train_nums(
        num, departure_date)
    if not trip_id:
        return False
    scheduled_departure_time = trip_scheduled_departure_time(
        trip_id, station)
    if not scheduled_departure_time:
        return False
    delay = compute_delay(scheduled_departure_time, departure_date)
    return delay


def check_random_trips_delay(yyyymmdd_date, limit=1000):
    """
    Mostly testing function
    """

    collection = mongo_get_collection("real_departures")
    departures = list(collection.find(
        {"scheduled_departure_day": yyyymmdd_date}).limit(limit))

    for departure in departures:
        scheduled_departure_day = departure["scheduled_departure_day"]
        scheduled_departure_time = departure["scheduled_departure_time"]
        station = departure["station_id"]
        num = departure["train_num"]
        print("SEARCH: num %s" % num)
        trip_id = get_trip_ids_from_day_and_train_nums(
            num, scheduled_departure_time)
        if not trip_id:
            continue
        print("Trip id: %s" % trip_id)
        delay = api_passage_information_to_delay(
            num, scheduled_departure_day, station)
        print("Delay: %s seconds" % delay)


if __name__ == '__main__':
    pass

    # Let's check for today
    #date_to_check = datetime.now().strftime("%Y%m%d")
    # check_random_trips_delay(date_to_check)
