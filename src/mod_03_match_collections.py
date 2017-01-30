import os
from os import sys, path
from datetime import datetime
import calendar
import logging

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from src.utils_misc import set_logging_conf
    set_logging_conf(log_name="mod_03_match.log")

from src.utils_rdb import rdb_connection, postgres_async_query_get_trip_ids
from src.utils_mongo import mongo_get_collection, mongo_async_update_items
from src.mod_02_query_schedule import get_departure_times_of_day_json_list, trip_scheduled_departure_time
from src.settings import BASE_DIR, data_path, gtfs_path, col_real_dep_unique
from multiprocessing.dummy import Pool as ThreadPool

logger = logging.getLogger(__name__)


# trip_id is unique for ONE DAY
# to know exactly the schedule of a train, you need to tell: trip_id AND day
# next, station to get time
def update_real_departures_mongo(yyyymmdd, threads=5):
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
    real_dep_on_day = list(real_departures_col.find(
        {"expected_passage_day": yyyymmdd}))

    logger.info("Found %d elements in real_departures collection." %
                len(real_dep_on_day))

    # PART 2 : GET TRIP_ID, SCHEDULED_DEPARTURE_TIME AND DELAY
    # PART 2.A : GET TRIP_ID
    def add_trip_id(item):
        try:
            logger.debug("Update train %s on station %s on day %s" %
                         (item["train_num"], item["station"], item["expected_passage_day"]))
            item_trip_id = api_train_num_to_trip_id(
                item["train_num"], yyyymmdd)
            if not item_trip_id:
                # If we can't find trip_id, we remove item from list
                logger.warn("Cannot find trip_id for element")
            else:
                item["trip_id"] = item_trip_id
                logger.info("Found trip_id")
                return item
        except Exception as e:
            real_dep_on_day.remove(item)
            logger.warn("Cannot find trip_id for element, exception %s" % e)
            item = None

    pool = ThreadPool(5)
    real_dep_on_day = pool.map(add_trip_id, real_dep_on_day)
    pool.close()
    pool.join()

    real_dep_on_day = list(filter(None, real_dep_on_day))

    logger.info("Found trip_id for %s elements." % len(real_dep_on_day))

    # PART 2.B : GET TRIP SCHEDULED_DEPARTURE_TIME, DELAY
    def add_schedule_and_delay(item):
        try:
            scheduled_departure_time = trip_scheduled_departure_time(
                item["trip_id"], item["station"])
            if not scheduled_departure_time:
                real_dep_on_day.remove(item)
                logger.warn("Cannot find schedule for element")
            else:
                # Reminder: item["date"] is real departure date
                delay = compute_delay(
                    scheduled_departure_time, item["date"])
                item["scheduled_departure_time"] = scheduled_departure_time
                item["delay"] = delay
                logger.info("Found schedule and delay")
                return item
        except Exception as e:
            logger.warn(
                "Cannot find schedule or delay for element: exception %s" % e)
            item = None

    pool = ThreadPool(5)
    real_dep_on_day = pool.map(add_schedule_and_delay, real_dep_on_day)
    pool.close()
    pool.join()
    real_dep_on_day = list(filter(None, real_dep_on_day))

    # PART 2.C : BUILD UPDATE OBJECTS FOR MONGO
    items_to_update = []
    for item in real_dep_on_day:
        try:
            item_to_update = (
                {"_id": item["_id"]},
                {"$set":
                 {"scheduled_departure_time": item["scheduled_departure_time"],
                  "trip_id": item["trip_id"],
                  "delay": item["delay"]
                  }
                 })
            items_to_update.append(item_to_update)
            logger.info("Item prepared successfully")
        except Exception as e:
            logger.warn(
                "Couldn't prepare element: %s, exception %s" % (item, e))
            continue

    logger.info("Real departures gathering finished. Beginning update.")
    logger.info("Gathered %d elements to update." % len(items_to_update))

    # PART 3: UPDATE ALL IN MONGO
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]
    update_chunks = chunks(items_to_update, 1000)
    for i, update_chunk in enumerate(update_chunks):
        logger.info(
            "Processing chunk number %d of 1000 elements to update." % i)
        mongo_async_update_items(col_real_dep_unique, update_chunk)


def api_train_num_to_trip_id(train_num, yyyymmdd_day, weekday=None):
    # Check parameters train_num and departure_day

    # Make query
    connection = rdb_connection()
    cursor = connection.cursor()
    query = "SELECT trip_id FROM trips_ext WHERE train_num='%s' AND start_date<='%s' AND end_date>='%s';" % (
        train_num, yyyymmdd_day, yyyymmdd_day)
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


if __name__ == '__main__':
    pass

    # Let's check for today
    # date_to_check = datetime.now().strftime("%Y%m%d")
    # check_random_trips_delay(date_to_check)
