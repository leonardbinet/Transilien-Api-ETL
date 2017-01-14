import pandas as pd
import os
import ipdb
from datetime import datetime
import calendar
from utils import mongo_get_collection


def get_flat_df():
    data_path = "../data/gtfs-lines-last"
    trips = pd.read_csv(os.path.join(data_path, "trips.txt"))
    calendar = pd.read_csv(os.path.join(data_path, "calendar.txt"))
    stop_times = pd.read_csv(os.path.join(data_path, "stop_times.txt"))
    stops = pd.read_csv(os.path.join(data_path, "stops.txt"))

    df_merged = stop_times.merge(trips, on="trip_id", how="left")
    df_merged = df_merged.merge(calendar, on="service_id", how="left")
    df_merged = df_merged.merge(stops, on="stop_id", how="left")

    df_merged["train_id"] = df_merged.trip_id.str.extract("^.{5}(\d{6})")
    df_merged["station_id"] = df_merged.stop_id.str.extract("DUA(\d{7})")
    return df_merged

# trip_id is unique for ONE DAY
# to know exactly the schedule of a train, you need to tell: trip_id AND day
# next, station to get time


def api_passage_information_to_trip_id(num, arrival_date, station=None, miss=None, term=None, df_merged=None, ignore_multiple=False):
    if not isinstance(df_merged, pd.core.frame.DataFrame):
        df_merged = get_flat_df()
    weekday = arrival_date_to_week_day(arrival_date)
    # Find possible trip_ids
    df_poss = df_merged[df_merged.train_id == num]
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
        print("Multiple trip ids found")
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
        df_merged = get_flat_df()
    # Station ids are not exactly the same: don't use last digit
    station = station[:-1]
    condition_trip = df_merged.trip_id == trip_id
    condition_station = df_merged.station_id == str(station)
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
        print("Multiple scheduled time found")
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


if __name__ == "__main__":
    # Example
    miss = "HAVA"
    term = "87281899"
    arrival_date = "01/02/2017 22:12"
    station = "87113803"
    num = "118622"

    df_merged = get_flat_df()
    delay = api_passage_information_to_delay(
        num, arrival_date, station, df_merged=df_merged)
