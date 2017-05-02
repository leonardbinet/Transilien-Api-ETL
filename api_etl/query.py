"""
Module used to query schedule data contained in relational databases.
"""

import logging
import collections
from datetime import datetime

import pandas as pd
from pynamodb.exceptions import DoesNotExist

# from sqlalchemy.orm import aliased
# from sqlalchemy.sql import func

if __name__ == '__main__':
    import logging.config
    logging.config.fileConfig('logging.conf')

from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter
from api_etl.utils_rdb import RdbProvider
from api_etl.utils_mongo import mongo_async_upsert_items
from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route,
    RealTimeDeparture
)


pd.options.mode.chained_assignment = None


class ResultSerializer():
    """ This class transforms a sqlalchemy result in an easy to manipulate
    object.
    The result can be:
    - an object containing rdb models instances: (StopTime,Trip,Calendar)
    - a model instance: StopTime or Trip or Calendar, etc

    If a StopTime is present, it can request RealTime to dynamo database,
    for the day given as parameter (today if none provided)
    """

    def __init__(self, raw_result):
        self._raw = raw_result

        if hasattr(raw_result, "_asdict"):
            # if sqlalchemy result, has _asdict method
            for key, value in raw_result._asdict().items():
                setattr(self, key, value)
        else:
            # or if sqlalchemy single model
            setattr(self, raw_result.__class__.__name__, raw_result)

        self._realtime_query_day = None
        self._realtime_found = None

    def get_nested_dict(self):
        return self._clean_extend_dict(self.__dict__)

    def get_flat_dict(self):
        return self._flatten(self.get_nested_dict())

    def has_stoptime(self):
        """Necessary to know if we should compute realtime requests.
        """
        return hasattr(self, "StopTime")

    def has_realtime(self):
        """ Returns:
        \n- None, if request not made (no stoptime, or not requested yet)
        \n- False, if request made (stoptime is present), but no realtime found
        \n- True, if stoptime is present, request has been made, and realtime
        has been found
        """
        return self._realtime_found

    def _clean_extend_dict(self, odict):
        ndict = {}
        for key, value in odict.items():
            if not key.startswith('_'):
                if isinstance(value, RealTimeDeparture):
                    # RealTimeDeparture has a __dict__ attribute, but
                    # it returns a dict with attribute_values as key
                    ndict[key] = self._clean_extend_dict(
                        value.attribute_values)
                elif hasattr(value, "__dict__"):
                    ndict[key] = self._clean_extend_dict(value.__dict__)
                elif isinstance(value, dict):
                    ndict[key] = self._clean_extend_dict(value)
                else:
                    ndict[key] = value
        return ndict

    def _flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_realtime_query_index(self, yyyymmdd):
        """Return (station_id, day_train_num) query index for real departures
        dynamo table.
        """
        assert self.has_stoptime()
        return self.StopTime.get_realtime_index(yyyymmdd=yyyymmdd)

    def set_realtime(self, yyyymmdd, realtime_object=None):
        """This method is used to propagate results when batch queries are
        performed by the ResultSetSerializer, or when a single query is made.

        It will add some meta information about it.
        """
        self._realtime_query_day = yyyymmdd

        if realtime_object:
            assert isinstance(realtime_object, RealTimeDeparture)
            # setattr(self._raw, 'RealTime', realtime_object)
            self.RealTime = realtime_object
            # self._raw.RealTime = realtime_object
            self._realtime_found = True
        else:
            self._realtime_found = False

    def perform_realtime_query(self, yyyymmdd, ignore_error=True):
        """This method will perform a query to dynamo to get realtime
        information about the StopTime in this result object only.
        \nIt requires a day, because a given trip_id can be on different dates.
        """
        assert self.has_stoptime()
        station_id, day_train_num = self.get_realtime_query_index(yyyymmdd)

        # Try to get it from dynamo
        try:
            realtime_object = RealTimeDeparture.get(
                hash_key=station_id,
                range_key=day_train_num
            )
            self.set_realtime(
                yyyymmdd=yyyymmdd,
                realtime_object=realtime_object
            )

        except DoesNotExist:
            self.set_realtime(
                yyyymmdd=yyyymmdd,
                realtime_object=False
            )
            logging.info("Realtime not found for %s, %s" %
                         (station_id, day_train_num))
            if not ignore_error:
                raise DoesNotExist

    def compute_trip_state(self, at_datetime=None):
        """ This method will add a dictionary in the "TripState" attribute.

        It will be made of:
        - at_time: the time considered
        - delay (between schedule and realtime) if realtime is found
        - passed_schedule: has train passed based on schedule information, at
        time passed as paramater (if none provided = now).
        - passed_realtime: has train passed based on realtime information.
        """
        if not at_datetime:
            at_datetime = get_paris_local_datetime_now()
        assert isinstance(at_datetime, datetime)
        self.TripState = {}

        self.TripState["at_datetime"] = at_datetime.strftime("%Y%m%d-%H:%M:%S")

        if self.has_stoptime():
            self.TripState["passed_schedule"] = self.StopTime\
                .has_passed(at_datetime=at_datetime)
        else:
            self.TripState["passed_schedule"] = "Unknown"

        if self.has_realtime():
            self.TripState["delay"] = self._delay_schedule_vs_realtime()
            self.TripState["passed_realtime"] = self.RealTime\
                .has_passed(at_datetime=at_datetime)
        else:
            self.TripState["delay"] = "Unknown"
            self.TripState["passed_realtime"] = "Unknown"

    def _delay_schedule_vs_realtime(self):
        """ Between scheduled 'stop time' departure time, and realtime expected
        departure time.
        """
        assert self.has_realtime()

        sdt = self.StopTime.departure_time
        # _realtime_query_day attribute is set when performing realtime query
        sdd = self._realtime_query_day
        rtdt = self.RealTime.expected_passage_time
        rtdd = self.RealTime.expected_passage_day
        # Schedule and realtime are both in special format
        # allowing hour to go up to 27
        delay = DateConverter(special_date=rtdd, special_time=rtdt)\
            .compute_delay_from(special_date=sdd, special_time=sdt)
        return delay

    def _normalize_realtime(self, ndict):
        """
        In order to have consistent output, all missing attributes are set to
        unknown if not present.
        """
        # Add all missing attributes of RealTimeDeparture object so all
        # elements have the same shape
        if "RealTime" not in ndict:
            ndict["RealTime"] = {}

        for key, value in RealTimeDeparture._get_attributes().items():
            if key not in ndict:
                ndict["RealTime"][key] = "Unknown"


class ResultSetSerializer():

    def __init__(self, raw_result, yyyymmdd=None):
        if isinstance(raw_result, list):
            self.results = list(map(ResultSerializer, raw_result))
        else:
            self.results = [ResultSerializer(raw_result)]

        self.yyyymmdd = yyyymmdd
        self.mongo_collection = "flat_stop_times"

    def _index_stoptime_results(self, yyyymmdd):
        """ Index elements containing a StopTime object.
        """
        self._indexed_results = {
            result.get_realtime_query_index(yyyymmdd): result
            for result in self.results
            if result.has_stoptime()
        }

    def get_nested_dicts(self, realtime_only=False):
        if realtime_only:
            return [x.get_nested_dict() for x in self.results if x.has_realtime()]
        else:
            return [x.get_nested_dict() for x in self.results]

    def get_flat_dicts(self, realtime_only=False):
        if realtime_only:
            return [x.get_flat_dict() for x in self.results if x.has_realtime()]
        else:
            return [x.get_flat_dict() for x in self.results]

    def batch_realtime_query(self, yyyymmdd):
        # 1: get all elements that have StopTime
        # 2: build all indexes (station_id, day_train_num)
        self._index_stoptime_results(yyyymmdd)
        # 3: send a batch request to get elements
        # 4: dispatch correcly answers
        item_keys = [key for key, value in self._indexed_results.items()]
        for item in RealTimeDeparture.batch_get(item_keys):
            index = (item.station_id, item.day_train_num)
            self._indexed_results[index].set_realtime(yyyymmdd, item)
        # 5: ResultSerializer instances objects are then already updated
        # and available under self.results

    def first_realtime_index(self):
        """Mostly for debugging: returns index of first result which has
        realtime.
        """
        for i, el in enumerate(self.results):
            if el.has_realtime():
                return i

    def compute_trip_states(self, at_datetime=None):
        for res in self.results:
            res.compute_trip_state(at_datetime=at_datetime)

    def save_in_mongo(self, collection=None, objects=None):
        index_fields = ["StopTime_trip_id", "Stop_stop_id"]
        collection = collection or self.mongo_collection
        objects = objects or self.get_flat_dicts()
        assert objects
        mongo_async_upsert_items(
            item_list=objects, collection=collection,
            index_fields=index_fields)


class DBQuerier():
    """ This class allows you to easily query information available in
    databases: both RDB containing schedules, and Dynamo DB containing
    real-time data.
    \nThe possible methods are:
    \n -services_of_day: returns a list of strings.
    \n -trip_stops: gives trips stops for a given trip_id.
    \n -station_trip_stops: gives trips stops for a given station_id (in gtfs
    format:7 digits).
    """

    def __init__(self, yyyymmdd=None):
        self.provider = RdbProvider()
        if not yyyymmdd:
            yyyymmdd = get_paris_local_datetime_now().strftime("%Y%m%d")
        else:
            # Will raise an error if wrong format
            datetime.strptime(yyyymmdd, "%Y%m%d")
        self.yyyymmdd = yyyymmdd

    def set_date(self, yyyymmdd):
        """Sets date that will define default date for requests.
        """
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")
        self.yyyymmdd = yyyymmdd

    def services_of_day(self, yyyymmdd=None):
        """Return all service_id's for a given day.
        """
        yyyymmdd = yyyymmdd or self.yyyymmdd
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")

        all_services = self.provider.get_session()\
            .query(Calendar.service_id)\
            .filter(Calendar.start_date <= yyyymmdd)\
            .filter(Calendar.end_date >= yyyymmdd)\
            .all()

        # Get service exceptions
        # 1 = service (instead of usually not)
        # 2 = no service (instead of usually yes)

        serv_add = self.provider.get_session()\
            .query(CalendarDate.service_id)\
            .filter(CalendarDate.date == yyyymmdd)\
            .filter(CalendarDate.exception_type == "1")\
            .all()

        serv_rem = self.provider.get_session()\
            .query(CalendarDate.service_id)\
            .filter(CalendarDate.date == yyyymmdd)\
            .filter(CalendarDate.exception_type == "2")\
            .all()

        serv_on_day = set(all_services)
        serv_on_day.update(serv_add)
        serv_on_day = serv_on_day - set(serv_rem)
        serv_on_day = map(lambda x: x[0], serv_on_day)
        serv_on_day = list(serv_on_day)

        return serv_on_day

    def trips_of_day(self, yyyymmdd, has_begun_at_hour=None):
        if has_begun_at_hour:
            results = self.provider.get_session()\
                .query(Trip.trip_id)\
                .filter(Trip.service_id == Calendar.service_id)\
                .filter(Trip.service_id.in_(self.services_of_day(yyyymmdd)))\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(StopTime.stop_sequence == "0")\
                .filter(StopTime.departure_time <= has_begun_at_hour)\
                .all()
            return list(map(lambda x: x[0], results))
        else:
            results = self.provider.get_session()\
                .query(Trip.trip_id)\
                .filter(Trip.service_id == Calendar.service_id)\
                .filter(Trip.service_id.in_(self.services_of_day(yyyymmdd)))\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .all()
            return list(map(lambda x: x[0], results))

    def stops_of_day(self, yyyymmdd, stops_only=False):
        if stops_only:
            results = self.provider.get_session()\
                .query(StopTime)\
                .filter(StopTime.trip_id.in_(self.trips_of_day(yyyymmdd)))\
                .all()
            return ResultSetSerializer(results, yyyymmdd=yyyymmdd)
        else:
            results = self.provider.get_session()\
                .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
                .filter(Trip.trip_id == StopTime.trip_id)\
                .filter(Stop.stop_id == StopTime.stop_id)\
                .filter(Trip.route_id == Route.route_id)\
                .filter(Agency.agency_id == Route.agency_id)\
                .filter(Calendar.service_id == Trip.service_id)\
                .filter(StopTime.trip_id.in_(self.trips_of_day(yyyymmdd)))\
                .all()
            return ResultSetSerializer(results, yyyymmdd=yyyymmdd)

    def trip_stops(self, trip_id, yyyymmdd=None):
        """Return all stops of a given trip, on a given day.
        \nInitially the returned object contains only schedule information.
        \nThen, it can be updated with realtime information.
        """
        yyyymmdd = yyyymmdd or self.yyyymmdd
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Trip.trip_id == trip_id)\
            .all()
        return ResultSetSerializer(results, yyyymmdd=yyyymmdd)

    def station_trips_stops(self, station_id, yyyymmdd=None):
        """Return all trip stops of a given station, on a given day.
        \n -station_id should be in 7 digits gtfs format
        \n -day is in yyyymmdd format
        \n
        \nInitially the returned object contains only schedule information.
        \nThen, it can be updated with realtime information.
        """
        yyyymmdd = yyyymmdd or self.yyyymmdd
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")

        station_id = str(station_id)
        assert len(station_id) == 7

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(StopTime.stop_id.like("%" + station_id))\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Calendar.service_id.in_(self.services_of_day(yyyymmdd)))\
            .all()

        return ResultSetSerializer(results, yyyymmdd=yyyymmdd)
