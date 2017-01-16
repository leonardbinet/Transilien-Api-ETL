import pandas as pd
import os
from os import sys, path
from datetime import datetime
import calendar
import ipdb

data_path = "../data/gtfs-lines-last"


def write_flat_df():
    trips = pd.read_csv(os.path.join(data_path, "trips.txt"))
    trips["train_id"] = trips["trip_id"].str.extract("^.{5}(\d{6})")

    calendar = pd.read_csv(os.path.join(data_path, "calendar.txt"))
    stop_times = pd.read_csv(os.path.join(data_path, "stop_times.txt"))
    stops = pd.read_csv(os.path.join(data_path, "stops.txt"))

    df_merged = stop_times.merge(trips, on="trip_id", how="left")
    df_merged = df_merged.merge(calendar, on="service_id", how="left")
    df_merged = df_merged.merge(stops, on="stop_id", how="left")

    df_merged["station_id"] = df_merged.stop_id.str.extract("DUA(\d{7})")

    useful = [
        "trip_id", "departure_time", "station_id",
        "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday",
        "start_date", "stop_date", "train_id"
    ]
    df_merged[useful].to_csv(os.path.join(data_path, "flat.csv"))
    return df_merged

# trip_id is unique for ONE DAY
# to know exactly the schedule of a train, you need to tell: trip_id AND day
# next, station to get time


def api_passage_information_to_trip_id(num, departure_date, station=None, miss=None, term=None, df_merged=None, ignore_multiple=False):
    if not isinstance(df_merged, pd.core.frame.DataFrame):
        df_merged = pd.read_csv(os.path.join(data_path, "flat.csv"))

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
        df_merged = pd.read_csv(os.path.join(data_path, "flat.csv"))
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


if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_mongo import mongo_get_collection

    # Example
    # miss = "HAVA"
    # term = "87281899"
    # departure_date = "01/02/2017 22:12"
    # station = "87113803"
    # num = "118622"

    collection = mongo_get_collection("departures")
    departures = list(collection.find({}).limit(100))
    # ipdb.set_trace()

    try:
        df_merged = pd.read_csv(os.path.join(data_path, "flat.csv"))
    except:
        print("Not found")
        df_merged = write_flat_df()

    for departure in departures:
        departure_date = departure["date"]["#text"]
        station = departure["station"]
        num = departure["num"]
        print("SEARCH: num %s" % num)
        trip_id = api_passage_information_to_trip_id(
            num, departure_date, df_merged=df_merged)
        if not trip_id:
            continue
        print("Trip id: %s" % trip_id)
        delay = api_passage_information_to_delay(
            num, departure_date, station, df_merged=df_merged)
        print("Delay: %s seconds" % delay)
