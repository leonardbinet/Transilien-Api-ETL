import pandas as pd
import os
from os import sys, path
from datetime import datetime
import calendar
import ipdb

data_path = "../data/gtfs-lines-last"


def write_flat_df():
    trips = pd.read_csv(os.path.join(data_path, "trips.txt"))
    trips["train_id"] = trips.trip_id.str.extract("^.{5}(\d{6})")
    calendar = pd.read_csv(os.path.join(data_path, "calendar.txt"))
    stop_times = pd.read_csv(os.path.join(data_path, "stop_times.txt"))
    stops = pd.read_csv(os.path.join(data_path, "stops.txt"))

    df_merged = stop_times.merge(trips, on="trip_id", how="left")
    df_merged = df_merged.merge(calendar, on="service_id", how="left")
    df_merged = df_merged.merge(stops, on="stop_id", how="left")

    df_merged["station_id"] = df_merged.stop_id.str.extract("DUA(\d{7})")
    df_merged.to_csv(os.path.join(data_path, "flat.csv"))
    return df_merged

# trip_id is unique for ONE DAY
# to know exactly the schedule of a train, you need to tell: trip_id AND day
# next, station to get time


def api_passage_information_to_trip_id(num, arrival_date, station=None, miss=None, term=None, df_merged=None, ignore_multiple=False):
    if not isinstance(df_merged, pd.core.frame.DataFrame):
        df_merged = pd.read_csv(os.path.join(data_path, "flat.csv"))
    weekday = arrival_date_to_week_day(arrival_date)
    # Find possible trip_ids
    df_poss = df_merged[df_merged.train_id == int(num)]
    df_poss = df_poss[df_poss[weekday] == 1]

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


def arrival_date_to_week_day(arrival_date):
    # format: "01/02/2017 22:12"
    arrival_date = datetime.strptime(arrival_date, "%d/%m/%Y %H:%M")
    weekday = calendar.day_name[arrival_date.weekday()]
    return weekday.lower()


def get_scheduled_arrival_time_from_trip_id_and_station(trip_id, station, df_merged=None, ignore_multiple=False):
    if not isinstance(df_merged, pd.core.frame.DataFrame):
        df_merged = pd.read_csv(os.path.join(data_path, "flat.csv"))
    # Station ids are not exactly the same: don't use last digit
    station = str(station)[:-1]
    condition_trip = df_merged.trip_id == str(trip_id)
    condition_station = df_merged.station_id == int(station)
    pot_scheduled_arrival_time = df_merged[condition_trip][
        condition_station].arrival_time.unique()
    pot_scheduled_arrival_time = list(pot_scheduled_arrival_time)
    n = len(pot_scheduled_arrival_time)
    if n == 0:
        print("No matching scheduled_arrival_time")
        return False
    elif n == 1:
        return pot_scheduled_arrival_time[0]
    else:
        print("Multiple scheduled time found: %d matches" % n)
        if ignore_multiple:
            return pot_scheduled_arrival_time[0]
        else:
            return False


def compute_delay(scheduled_arrival_time, real_arrival_date):
    # real_arrival_date = "01/02/2017 22:12"
    # scheduled_arrival_time = '22:12:00'
    # Lets suppose it is always the same day (don't take into account
    # overlapping at midnight)
    real_arrival_date = datetime.strptime(arrival_date, "%d/%m/%Y %H:%M")
    scheduled_arrival_date = datetime.strptime(
        scheduled_arrival_time, "%H:%M:%S")
    scheduled_arrival_date.replace(year=real_arrival_date.year)
    scheduled_arrival_date.replace(month=real_arrival_date.month)
    scheduled_arrival_date.replace(day=real_arrival_date.day)

    # If late: delay is positive, if in advance, it is negative
    delay = real_arrival_date - scheduled_arrival_date
    return delay.seconds


def api_passage_information_to_delay(num, arrival_date, station, miss=None, term=None, df_merged=None, ignore_multiple=False):
    trip_id = api_passage_information_to_trip_id(
        num, arrival_date, df_merged=df_merged)
    if not trip_id:
        return False
    scheduled_arrival_time = get_scheduled_arrival_time_from_trip_id_and_station(
        trip_id, station, df_merged=df_merged)
    if not scheduled_arrival_time:
        return False
    delay = compute_delay(scheduled_arrival_time, arrival_date)
    return delay


if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_mongo import mongo_get_collection
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    # Example
    miss = "HAVA"
    term = "87281899"
    arrival_date = "01/02/2017 22:12"
    station = "87113803"
    num = "118622"

    collection = mongo_get_collection("departures")
    departures = list(collection.find({}).limit(100))
    # ipdb.set_trace()

    try:
        df_merged = pd.read_csv(os.path.join(data_path, "flat.csv"))
    except:
        print("Not found")
        df_merged = write_flat_df()

    for departure in departures:
        arrival_date = departure["date"]["#text"]
        station = departure["station"]
        num = departure["num"]
        print("SEARCH: num %s" % num)
        trip_id = api_passage_information_to_trip_id(
            num, arrival_date, df_merged=df_merged)
        if not trip_id:
            continue
        print("Trip id: %s" % trip_id)
        delay = api_passage_information_to_delay(
            num, arrival_date, station, df_merged=df_merged)
        print("Delay: %s seconds" % delay)
