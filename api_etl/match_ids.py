"""
Module used to match data with different identification formats: more specifically, Transilien's API gives train_num, and GTFS schedule files provide a trip_id.
"""

from os import sys, path
import logging
from multiprocessing.dummy import Pool as ThreadPool

from api_etl.utils_rdb import rdb_connection
from api_etl.utils_mongo import mongo_get_collection, mongo_async_update_items
from api_etl.utils_misc import compute_delay, chunks
from api_etl.query_schedule import trip_scheduled_departure_time
from api_etl.settings import col_real_dep_unique

logger = logging.getLogger(__name__)


# Item update functions

def add_trip_id(item):
    """
    Takes an item (dictionary) as parameter.
    - either finds trip_id and add it in dictionary, and return dict updated.
    - either return None
    """
    try:
        logger.debug(
            "Update train %s on station %s on day %s",
            item["train_num"], item["station"],
            item["expected_passage_day"])
        item_trip_id = api_train_num_to_trip_id(
            item["train_num"], item["expected_passage_day"]
        )
        if not item_trip_id:
            # If we can't find trip_id, we remove item from list
            logger.debug("Cannot find trip_id for element")
        else:
            item["trip_id"] = item_trip_id
            logger.debug("Found trip_id")
            return item
    except Exception as e:
        logger.debug(
            "Cannot find trip_id for element, exception %s",
            e.with_traceback
        )


def add_schedule_and_delay(item):
    """
    Takes an item (dictionary) as parameter.
    - either finds scheduled_departure_time and delay and add it in dictionary, and return dict updated.
    - either return None
    """
    try:
        scheduled_departure_time = trip_scheduled_departure_time(
            item["trip_id"], item["station"])
        if not scheduled_departure_time:
            logger.debug("Cannot find schedule for element")
        else:
            # Reminder: item["expected_passage_time"] is real departure
            # time
            delay = compute_delay(
                scheduled_departure_time, item["expected_passage_time"])
            item["scheduled_departure_time"] = scheduled_departure_time
            # Set as a string to be json encoded
            item["delay"] = str(delay)
            logger.debug("Found schedule and delay")
            return item
    except Exception as e:
        logger.debug(
            "Cannot find schedule or delay for element: exception %s", e)


def build_mongo_update_object(item):
    """
    Takes an item (dictionary) as parameter.
    - either create successfully update object for mongo
    - either return None
    """
    try:
        item_to_update = (
            {"_id": item["_id"]},
            {"$set":
             {"scheduled_departure_time": item["scheduled_departure_time"],
              "trip_id": item["trip_id"],
              "delay": item["delay"]
              }
             })
        logger.debug("Item prepared successfully")
        return item_to_update
    except Exception as e:
        logger.error(
            "Couldn't prepare element: %s, exception %s", item, e)


def update_real_departures_mongo(yyyymmdd, threads=5, limit=1000000, one_station=False, dryrun=False):
    """
    Update real_departures with scheduled departure times for a given request day:
    - iterate over all elements in real departures collection for that day
    (defined in settings 'col_real_dep_unique')
    - find their real trip_id
    - find their scheduled departure time
    - compute delay
    - build update objects
    - compute mongo update queries for collection 'col_real_dep_unique'
    (defined in settings, same as first step)
    """

    # PART 1 : GET ALL ELEMENTS TO UPDATE FROM MONGO
    real_departures_col = mongo_get_collection(col_real_dep_unique)

    if not one_station:
        real_dep_on_day = list(real_departures_col.find(
            {"expected_passage_day": yyyymmdd}).limit(limit))
        logger.info("Found %d elements in real_departures collection.",
                    len(real_dep_on_day))
    else:
        one_station = str(one_station)
        real_dep_on_day = list(real_departures_col.find(
            {"expected_passage_day": yyyymmdd, "station": one_station}).limit(limit))
        logger.info(
            "Found %d elements in real_departures collection for station %s.",
            len(real_dep_on_day), one_station
        )

    # PART 2 : GET TRIP_ID, SCHEDULED_DEPARTURE_TIME AND DELAY
    # PART 2.A : GET TRIP_ID
    pool = ThreadPool(5)
    real_dep_on_day = pool.map(add_trip_id, real_dep_on_day)
    pool.close()
    pool.join()
    real_dep_on_day = list(filter(None, real_dep_on_day))
    logger.info("Found trip_id for %s elements.", len(real_dep_on_day))

    # PART 2.B : GET TRIP SCHEDULED_DEPARTURE_TIME, DELAY
    pool = ThreadPool(5)
    real_dep_on_day = pool.map(add_schedule_and_delay, real_dep_on_day)
    pool.close()
    pool.join()
    real_dep_on_day = list(filter(None, real_dep_on_day))

    # PART 2.C : BUILD UPDATE OBJECTS FOR MONGO
    items_to_update = list(map(build_mongo_update_object, real_dep_on_day))
    items_to_update = list(filter(None, items_to_update))
    logger.info("Real departures gathering finished. Beginning update.")
    logger.info("Gathered %d elements to update.", len(items_to_update))

    # PART 3: UPDATE ALL IN MONGO
    if not dryrun:
        update_chunks = chunks(items_to_update, 1000)
        for i, update_chunk in enumerate(update_chunks):
            logger.info(
                "Processing chunk number %d (chunks of max 1000) containing %d elements to update.",
                i, len(update_chunk)
            )
            mongo_async_update_items(col_real_dep_unique, update_chunk)
    else:
        logger.info("Dryrun is on: nothing is updated on mongo")


def api_train_num_to_trip_id(train_num, yyyymmdd_day, weekday=None):
    # Check parameters train_num and departure_day
    # Make query
    connection = rdb_connection()
    cursor = connection.cursor()
    query = "SELECT trip_id FROM trips_ext WHERE train_num='%s' AND start_date<='%s' AND end_date>='%s';" % (
        train_num, yyyymmdd_day, yyyymmdd_day)
    cursor.execute(query, (train_num, yyyymmdd_day, yyyymmdd_day))
    trip_ids = cursor.fetchone()

    # Check number of results
    if not trip_ids:
        logger.debug("No matching trip_id")
        connection.close()
        return False
    elif len(trip_ids) == 1:
        trip_id = trip_ids[0]
        connection.close()
        logger.debug("Found trip_id: %s", trip_ids)
        return trip_id
    else:
        logger.debug("Multiple trip_ids found: %d matches: %s",
                     len(trip_ids), trip_ids)
        connection.close()
        return False

"""
# NOT USED FOR NOW

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
"""
