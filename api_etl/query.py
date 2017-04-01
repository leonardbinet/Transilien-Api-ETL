"""
Module used to query schedule data contained in relational databases.
"""

import logging
import collections
from datetime import datetime

import pandas as pd

from api_etl.utils_misc import get_paris_local_datetime_now
from api_etl.utils_rdb import RdbProvider
from api_etl.utils_mongo import mongo_async_upsert_items
from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route,
    RealTimeDeparture
)


logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


class ResultSerializer():
    """ This class transforms a sqlalchemy result in an easy to manipulate
    object.
    \nIt will:
    \n-set result as a nested dict
    \n-set result as flat dict
    \n-if a StopTime is present, it will request RealTime to dynamo database,
    for the day given as parameter (today if none provided)
    """

    def __init__(self, raw_result):
        self._raw = raw_result

    def get_nested_dict(self, realtime=True):
        # Sqlalchemy results have a _asdict() method.
        nested_dict = self._clean_extend_dict(self._raw._asdict())
        if realtime:
            return self._extend_dict_with_realtime(nested_dict)
        else:
            return nested_dict

    def get_flat_dict(self, realtime=True):
        return self._flatten(self.get_nested_dict(realtime=realtime))

    def has_stoptime(self):
        return hasattr(self._raw, "StopTime")

    def has_realtime(self):
        """ Returns:
        \n- None, if request not made (no stoptime, or not requested yet)
        \n- False, if request made (stoptime is present), but no realtime found
        \n- True, if stoptime is present, request has been made, and realtime
        has been found
        """
        if not hasattr(self._raw, "StopTime"):
            # No StopTime present
            return None

        if not hasattr(self._raw.StopTime, "realtime_found"):
            # StopTime object present, but request not made
            return None

        return self._raw.StopTime.realtime_found

    def _clean_extend_dict(self, odict):
        ndict = {}
        for key, value in odict.items():
            if not key.startswith('_'):
                if hasattr(value, "__dict__"):
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

    def perform_realtime_query(self, yyyymmdd):
        """This method will perform a query to dynamo to get realtime
        information about the StopTime in this result object only.
        \nIt requires a day, because a given trip_id can be on different dates.

        """
        assert self.has_stoptime()
        # Update StopTime object
        self._raw.StopTime.get_realtime_info(yyyymmdd=yyyymmdd)

    def get_realtime_query_index(self, yyyymmdd):
        """Query index depends on the requested day.
        """
        assert self.has_stoptime()
        return self._raw.StopTime.get_realtime_index(yyyymmdd=yyyymmdd)

    def set_realtime(self, realtime):
        """This method will set realtime object in as a StopTime attribute."""
        assert self.has_stoptime()
        assert isinstance(realtime, RealTimeDeparture)
        # Update StopTime object
        self._raw.StopTime.set_realtime(realtime)

    def _extend_dict_with_realtime(self, nested_dict):
        """
        In order to have consistent output, all missing attributes are set to
        unknown if not present.
        \nIt will add information to nested_dict, and recompute flat_dict
        """
        if not self.has_stoptime():
            return nested_dict

        try:
            nested_dict["RealTime"] = self._raw.StopTime._realtime_dict
            nested_dict["Delay"] = self._raw.StopTime.delay
            nested_dict["Passed"] = self._raw.StopTime.passed

        except (KeyError, AttributeError):
            nested_dict["RealTime"] = {}
            nested_dict["Delay"] = "Unknown"
            nested_dict["Passed"] = "Unknown"

        # Add all missing attributes of RealTimeDeparture object so all
        # elements have the same shape
        for key, value in RealTimeDeparture._get_attributes().items():
            if key not in nested_dict["RealTime"]:
                nested_dict["RealTime"][key] = "Unknown"

        return nested_dict


class ResultSetSerializer():

    def __init__(self, raw_result, yyyymmdd=None):
        if isinstance(raw_result, list):
            self._raws = raw_result
        else:
            self._raws = [raw_result]

        self.yyyymmdd = yyyymmdd
        self.results = list(map(ResultSerializer, self._raws))
        self.mongo_collection = "flat_stop_times"

    def _index_stoptime_results(self, yyyymmdd):
        """ Index elements containing a StopTime object.
        """
        self._indexed_results = {
            result.get_realtime_query_index(yyyymmdd): result
            for result in self.results
            if result.has_stoptime()
        }

    def get_nested_dicts(self, realtime_only=False, normalize=True):
        if realtime_only:
            return [x.get_nested_dict(normalize) for x in self.results if x.has_realtime()]
        else:
            return [x.get_nested_dict(normalize) for x in self.results]

    def get_flat_dicts(self, realtime_only=False, normalize=True):
        if realtime_only:
            return [x.get_flat_dict(normalize) for x in self.results if x.has_realtime()]
        else:
            return [x.get_flat_dict(normalize) for x in self.results]

    def batch_realtime_query(self, yyyymmdd):
        # 1: get all elements that have StopTime
        # 2: build all indexes (station_id, day_train_num)
        self._index_stoptime_results(yyyymmdd)
        # 3: send a batch request to get elements
        # 4: dispatch correcly answers
        self._indexed_results
        item_keys = [key for key, value in self._indexed_results.items()]
        for item in RealTimeDeparture.batch_get(item_keys):
            index = (item.station_id, item.day_train_num)
            self._indexed_results[index].set_realtime(item)
        # 5: make results available
        self.results = [value for key, value in self._indexed_results.items()]

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

        self.resultset = ResultSetSerializer(results, yyyymmdd=yyyymmdd)
        return self.resultset
