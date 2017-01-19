import os
from os import sys, path
import pandas as pd
from datetime import datetime
import ipdb
import calendar
import logging

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.utils_mongo import mongo_get_collection
from src.mod_02_find_schedule import get_departure_times_df_of_day

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(BASE_DIR, "data")

# CONFIG
gtfs_path = os.path.join(data_path, "gtfs-lines-last")


# trip_id is unique for ONE DAY
# to know exactly the schedule of a train, you need to tell: trip_id AND day
# next, station to get time

def api_passage_information_to_trip_id(num, departure_date, station=None, miss=None, term=None, df_merged=None, ignore_multiple=False):
    if not isinstance(df_merged, pd.core.frame.DataFrame):
        df_merged = pd.read_csv(os.path.join(gtfs_path, "flat.csv"))

    weekday = departure_date_to_week_day(departure_date)
    yyyymmdd_format = departure_date_to_yyyymmdd_date(departure_date)
    # Find possible trip_ids
    cond1 = df_merged["train_id"] == int(num)
    cond2 = df_merged[weekday] == 1
    cond3 = df_merged["start_date"] <= int(yyyymmdd_format)
    cond4 = df_merged["end_date"] >= int(yyyymmdd_format)

    df_poss = df_merged[cond1][cond2][cond3][cond4]

    potential_trip_ids = list(df_poss.trip_id.unique())
    # ipdb.set_trace()
    n = len(potential_trip_ids)
    if n == 0:
        print("No matching trip id")
        return False
    elif n == 1:
        return potential_trip_ids[0]
    else:
        print("Multiple trip ids found: %d matches" % n)
        if ignore_multiple:
            return potential_trip_ids[0]
        else:
            return False


def departure_date_to_week_day(departure_date):
    # format: "01/02/2017 22:12"
    departure_date = datetime.strptime(departure_date, "%d/%m/%Y %H:%M")
    weekday = calendar.day_name[departure_date.weekday()]
    return weekday.lower()


def departure_date_to_yyyymmdd_date(departure_date):
    # format: "01/02/2017 22:12" to "2017"
    departure_date = datetime.strptime(departure_date, "%d/%m/%Y %H:%M")
    new_format = departure_date.strftime("%Y%m%d")
    return new_format


def get_scheduled_departure_time_from_trip_id_and_station(trip_id, station, df_merged=None, ignore_multiple=False):
    if not isinstance(df_merged, pd.core.frame.DataFrame):
        df_merged = pd.read_csv(os.path.join(gtfs_path, "flat.csv"))
    # Station ids are not exactly the same: don't use last digit
    station = str(station)[:-1]
    condition_trip = df_merged["trip_id"] == str(trip_id)
    condition_station = df_merged["station_id"] == int(station)

    pot_scheduled_departure_time = df_merged[condition_trip][
        condition_station]["departure_time"].unique()
    pot_scheduled_departure_time = list(pot_scheduled_departure_time)
    n = len(pot_scheduled_departure_time)
    if n == 0:
        print("No matching scheduled_departure_time")
        return False
    elif n == 1:
        return pot_scheduled_departure_time[0]
    else:
        print("Multiple scheduled time found: %d matches" % n)
        if ignore_multiple:
            return pot_scheduled_departure_time[0]
        else:
            return False


def compute_delay(scheduled_departure_time, real_departure_date):
    # real_departure_date = "01/02/2017 22:12"
    # scheduled_departure_time = '22:12:00'
    # Lets suppose it is always the same day (don't take into account
    # overlapping at midnight)
    real_departure_date = datetime.strptime(
        real_departure_date, "%d/%m/%Y %H:%M")

    scheduled_departure_date = datetime.strptime(
        scheduled_departure_time, "%H:%M:%S")

    scheduled_departure_date.replace(year=real_departure_date.year)
    scheduled_departure_date.replace(month=real_departure_date.month)
    scheduled_departure_date.replace(day=real_departure_date.day)

    # If late: delay is positive, if in advance, it is negative
    delay = real_departure_date - scheduled_departure_date
    return delay.seconds


def api_passage_information_to_delay(num, departure_date, station, miss=None, term=None, df_merged=None, ignore_multiple=False):
    trip_id = api_passage_information_to_trip_id(
        num, departure_date, df_merged=df_merged)
    if not trip_id:
        return False
    scheduled_departure_time = get_scheduled_departure_time_from_trip_id_and_station(
        trip_id, station, df_merged=df_merged)
    if not scheduled_departure_time:
        return False
    delay = compute_delay(scheduled_departure_time, departure_date)
    return delay


def check_random_trips_delay(yyyymmdd_date):
    """
    Mostly testing function
    """

    collection = mongo_get_collection("real_departures")
    departures = list(collection.find({}).limit(10))

    df_dep_times = get_departure_times_df_of_day(yyyymmdd_date)

    for departure in departures:
        departure_date = departure["date"]
        station = departure["station_id"]
        num = departure["train_num"]
        print("SEARCH: num %s" % num)
        trip_id = api_passage_information_to_trip_id(
            num, departure_date, df_merged=df_dep_times)
        if not trip_id:
            continue
        print("Trip id: %s" % trip_id)
        delay = api_passage_information_to_delay(
            num, departure_date, station, df_merged=df_dep_times)
        print("Delay: %s seconds" % delay)


if __name__ == '__main__':
    from src.task_01_extract import get_station_ids

    # Let's check for today
    date_to_check = datetime.now().strftime("%Y%m%d")
    check_random_trips_delay(date_to_check)
