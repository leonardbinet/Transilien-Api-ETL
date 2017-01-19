import pandas as pd
import os
from os import sys, path
from datetime import datetime
import calendar
import ipdb
import zipfile
from urllib.request import urlretrieve

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_mongo import mongo_get_collection

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(BASE_DIR, "data")


# CONFIG
gtfs_path = os.path.join(data_path, "gtfs-lines-last")
gtfs_csv_url = 'https://ressources.data.sncf.com/explore/dataset/sncf-transilien-gtfs/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'


def download_gtfs_files():
    df_links_gtfs = pd.read_csv(gtfs_csv_url)

    for link in df_links_gtfs["file"].values:
        local_filename, headers = urlretrieve(link)
        # Get name in header and remove the ".zip"
        extracted_data_folder_name = headers.get_filename().split(".")[0]
        with zipfile.ZipFile(local_filename, "r") as zip_ref:
            full_path = os.path.join(data_path, extracted_data_folder_name)
            zip_ref.extractall(path=full_path)


def write_flat_stop_times_df():
    try:
        trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
        calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))
        stop_times = pd.read_csv(path.join(gtfs_path, "stop_times.txt"))
        stops = pd.read_csv(path.join(gtfs_path, "stops.txt"))

    except OSError:
        print("Could not load files: download files from the internet.")
        download_gtfs_files()

        trips = pd.read_csv(path.join(gtfs_path, "trips.txt"))
        calendar = pd.read_csv(path.join(gtfs_path, "calendar.txt"))
        stop_times = pd.read_csv(path.join(gtfs_path, "stop_times.txt"))
        stops = pd.read_csv(path.join(gtfs_path, "stops.txt"))

    trips["train_id"] = trips["trip_id"].str.extract("^.{5}(\d{6})")

    df_merged = stop_times.merge(trips, on="trip_id", how="left")
    df_merged = df_merged.merge(calendar, on="service_id", how="left")
    df_merged = df_merged.merge(stops, on="stop_id", how="left")

    df_merged["station_id"] = df_merged.stop_id.str.extract("DUA(\d{7})")

    useful = [
        "trip_id", "departure_time", "station_id",
        "monday", "tuesday", "wednesday",
        "thursday", "friday", "saturday", "sunday",
        "start_date", "end_date", "train_id"
    ]
    df_merged[useful].to_csv(os.path.join(gtfs_path, "flat.csv"))
    return df_merged[useful]

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


def get_services_of_day(yyyymmdd_format):
    all_services = pd.read_csv(os.path.join(gtfs_path, "calendar.txt"))
    datetime_format = datetime.strptime(yyyymmdd_format, "%Y%m%d")
    weekday = calendar.day_name[datetime_format.weekday()].lower()

    cond1 = all_services[weekday] == 1
    cond2 = all_services["start_date"] <= int(yyyymmdd_format)
    cond3 = all_services["end_date"] >= int(yyyymmdd_format)

    matching_services = all_services[cond1][cond2][cond3]

    return list(matching_services["service_id"].values)


def get_trips_of_day(yyyymmdd_format):
    all_trips = pd.read_csv(os.path.join(gtfs_path, "trips.txt"))
    services_on_day = get_services_of_day(
        yyyymmdd_format)
    trips_condition = all_trips["service_id"].isin(services_on_day)
    trips_on_day = list(all_trips[trips_condition]["trip_id"].unique())
    return trips_on_day


def get_departure_times_df_of_day(yyyymmdd_format, stop_filter=None, station_filter=None):
    """
    stop_filter is a list of stops you want, it must be in GTFS format:
    station_filter is a list of stations you want, it must be api format
    """

    all_stop_times = pd.read_csv(os.path.join(gtfs_path, "stop_times.txt"))
    trips_on_day = get_trips_of_day(yyyymmdd_format)
    cond1 = all_stop_times["trip_id"].isin(trips_on_day)
    matching_stop_times = all_stop_times[cond1]

    matching_stop_times["train_id"] = matching_stop_times[
        "trip_id"].str.extract("^.{5}(\d{6})")
    matching_stop_times["station_id"] = matching_stop_times[
        "stop_id"].str.extract("DUA(\d{7})")
    matching_stop_times["day"] = yyyymmdd_format

    if stop_filter:
        cond2 = matching_stop_times["stop_id"].isin(stop_filter)
        matching_stop_times = matching_stop_times[cond2]

    if station_filter:
        cond3 = matching_stop_times["station_id"].isin(station_filter)
        matching_stop_times = matching_stop_times[cond3]

    return matching_stop_times


def check_random_trips_delay():
    """
    Mostly testing function
    """
    collection = mongo_get_collection("departures")
    departures = list(collection.find({}).limit(10))

    try:
        df_merged = pd.read_csv(path.join(gtfs_path, "flat.csv"))
    except:
        print("Not found")
        write_flat_stop_times_df()
        df_merged = pd.read_csv(path.join(gtfs_path, "flat.csv"))

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


if __name__ == '__main__':
    from src.task_01_extract import get_station_ids

    do_tests = True

    if do_tests:
        print("##### 1ST PART ######")
        check_random_trips_delay()

        print("##### 2ND PART ######")
        yyyymmdd_format = "20170119"
        station_ids_uic7 = get_station_ids(id_format="UIC7")
        stop_times = get_departure_times_df_of_day(yyyymmdd_format)
        filtered_stop_times = get_departure_times_df_of_day(
            yyyymmdd_format, station_filter=station_ids_uic7[:10])
