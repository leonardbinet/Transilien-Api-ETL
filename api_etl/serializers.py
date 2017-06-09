""" Classes used to serialize results of db queries.

Two main parts:
- ResultSerializer, ResultSetSerializer: serializers to serialize DB queries:
    an important part is the ability to extend schedule results with realtime
    information.

- FullStopTimeSerializer and directs Serializers: raw serializers:
    main ability is to provide a suitable serializer for django rest api,
    especially for pagination purposes.

    These are created through a Class Factory transforming model classes into
    serializer classes.
"""

import logging
import collections
from datetime import datetime

import pandas as pd
from rest_framework import serializers

from pynamodb.exceptions import DoesNotExist

from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter
from api_etl.utils_mongo import mongo_async_upsert_items
from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route,
    RealTimeDeparture
)

pd.options.mode.chained_assignment = None


def ModelToSerializerFactory(class_name, ExtractedClass):
    """ Transforms a model class in a corresponding Serializer class
    """
    BaseClass = serializers.Serializer

    def __init__(self, **kwargs):
        BaseClass.__init__(self, **kwargs)

    class_body = {"__init__": __init__}

    for key, value in ExtractedClass.__dict__.items():
        # for all non-hidden attributes
        if not key.startswith("_") and not callable(value):
            # set them as CharFields
            class_body[key] = serializers.CharField(
                max_length=300,
                required=False
            )

    newclass = type(class_name, (BaseClass,), class_body)
    return newclass

# Declaring my new serializers
# Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route, RealTimeDeparture

CalendarSerializer = ModelToSerializerFactory("CalendarSerializer", Calendar)
CalendarDateSerializer = ModelToSerializerFactory(
    "CalendarDateSerializer", CalendarDate)
TripSerializer = ModelToSerializerFactory("TripSerializer", Trip)
StopTimeSerializer = ModelToSerializerFactory("StopTimeSerializer", StopTime)
StopSerializer = ModelToSerializerFactory("StopSerializer", Stop)
AgencySerializer = ModelToSerializerFactory("AgencySerializer", Agency)
RouteSerializer = ModelToSerializerFactory("RouteSerializer", Route)
AgencySerializer = ModelToSerializerFactory("AgencySerializer", Agency)
RealTimeDepartureSerializer = ModelToSerializerFactory(
    "RealTimeDepartureSerializer", RealTimeDeparture)


class FullStopTimeSerializer(serializers.Serializer):
    Calendar = CalendarSerializer(required=False)
    CalendarDate = CalendarDateSerializer(required=False)
    Trip = TripSerializer(required=False)
    StopTime = StopTimeSerializer()
    Stop = StopSerializer(required=False)
    Agency = AgencySerializer(required=False)
    Route = RouteSerializer(required=False)
    RealTime = RealTimeDepartureSerializer(required=False)


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

    def batch_realtime_query(self, yyyymmdd=None):
        yyyymmdd = yyyymmdd or self.yyyymmdd
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
