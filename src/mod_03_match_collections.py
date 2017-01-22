import os
from os import sys, path
import pandas as pd
from datetime import datetime
import calendar
import logging

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_misc import set_logger_conf
    set_logger_conf(log_name="mod_03_match.log")

from src.utils_mongo import mongo_get_collection
from src.mod_02_find_schedule import get_departure_times_of_day_json_list, get_flat_departures_times_df, trip_scheduled_departure_time
from src.settings import BASE_DIR, data_path, gtfs_path

logger = logging.getLogger(__name__)


# trip_id is unique for ONE DAY
# to know exactly the schedule of a train, you need to tell: trip_id AND day
# next, station to get time

def get_trip_ids_from_day_and_train_nums(train_num_list, departure_date):
    df_flat = get_flat_departures_times_df()

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


def compute_delay(scheduled_departure_day, scheduled_departure_time, real_departure_date):
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
    # For day, might be different (after midnight) => Last two digits
    scheduled_day = scheduled_departure_day[-2:]
    scheduled_departure_date.replace(day=scheduled_day)

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
    from src.task_01_extract import get_station_ids

    # Let's check for today
    date_to_check = datetime.now().strftime("%Y%m%d")
    check_random_trips_delay(date_to_check)
