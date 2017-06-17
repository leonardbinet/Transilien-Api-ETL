""" Classes used to manipulate results of db queries.

Two main parts:
- ResultSerializer, ResultSetSerializer: serializers to serialize DB queries:
    an important part is the ability to extend schedule results with realtime
    information.

- NestedSerializer and directs Serializers: raw serializers:
    main ability is to provide a suitable serializer for django rest api,
    especially for pagination purposes.

    These are created through a Class Factory transforming models into
    serializers.
"""

import logging
import collections

import pandas as pd
from pynamodb.exceptions import DoesNotExist

from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter
from api_etl.models import RealTimeDeparture, StopTime

pd.options.mode.chained_assignment = None


class StopTimeState:
    """Used to compute StopTime state at a given time, comparing StopTime
    (schedule) vs RealTime.
    """

    # define required fields used to build serializer
    # these class attributes will be overriden by instances attributes
    at_datetime = None
    passed_schedule = None
    passed_realtime = None
    delay = None

    def __init__(self, at_datetime, scheduled_day, stoptime, realtime=None):
        assert isinstance(stoptime, StopTime)
        if realtime:
            assert isinstance(realtime, RealTimeDeparture)
        self._at_datetime = at_datetime
        self.at_datetime = at_datetime.strftime("%Y%m%d-%H:%M:%S")
        self._scheduled_day = scheduled_day
        self._StopTime = stoptime
        if realtime:
            assert isinstance(realtime, RealTimeDeparture)
        self._RealTime = realtime

        self.passed_schedule = self._StopTime\
                ._has_passed(at_datetime=at_datetime)

        if realtime:
            self._compute_delay()
            self.passed_realtime = self._RealTime\
                ._has_passed(at_datetime=at_datetime)
        else:
            self.delay = None
            self.passed_realtime = None

    def __repr__(self):
        return "<StopTimeState(delay='%s', passed_schedule='%s', passed_realtime='%s',  at_datetime='%s', " \
               "_scheduled_day='%s')>"\
            % (self.delay, self.passed_schedule, self.passed_realtime, self.at_datetime, self._scheduled_day)

    def __str__(self):
        return self.__repr__()

    def _has_realtime(self):
        return not not self._RealTime

    def _compute_delay(self):
        """ Between scheduled 'stop time' departure time, and realtime expected
        departure time.
        """
        assert self._RealTime is not None

        sdt = self._StopTime.departure_time
        # _realtime_query_day attribute is set when performing realtime query
        sdd = self._scheduled_day
        rtdt = self._RealTime.expected_passage_time
        rtdd = self._RealTime.expected_passage_day
        # Schedule and realtime are both in special format
        # allowing hour to go up to 27
        delay = DateConverter(special_date=rtdd, special_time=rtdt)\
            .compute_delay_from(special_date=sdd, special_time=sdt)
        self.delay = delay


class SingleResult:
    """ This class transforms a sqlalchemy result in an easy to manipulate
    object.
    The result can be:
    - an object containing rdb models instances: (StopTime,Trip,Calendar)
    - a model instance: StopTime or Trip or Calendar, etc

    If a StopTime is present, it has multiple capabilities:
    - it can request RealTime to dynamo database,
    for the day given as parameter (today if none provided)
    - it can compute TripPredictor based on realtime information
    """

    def __init__(self, raw_result, scheduled_day):
        self._raw = raw_result

        if hasattr(raw_result, "_asdict"):
            # if sqlalchemy nested result, has _asdict method
            for key, value in raw_result._asdict().items():
                setattr(self, key, value)
        else:
            # or if sqlalchemy single model
            setattr(self, raw_result.__class__.__name__, raw_result)

        self._realtime_query_day = None
        self._realtime_found = None
        self._scheduled_day = scheduled_day

    def __repr__(self):
        return "<SingleResult(has_stoptime='%s', has_realtime='%s', " \
               "realtime_found='%s', scheduled_day='%s')>"\
            % (self.has_stoptime(), self.has_realtime(), self._realtime_found, self._scheduled_day)

    def __str__(self):
        return self.__repr__()

    def get_nested_dict(self):
        return self._clean_extend_dict(self.__dict__)

    def get_flat_dict(self):
        return self._flatten(self.get_nested_dict())

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

    def get_realtime_query_index(self, scheduled_day):
        """Return (station_id, day_train_num) query index for real departures
        dynamo table.
        :param scheduled_day:
        """
        assert self.has_stoptime()
        return self.StopTime._get_realtime_index(scheduled_day=scheduled_day)

    def set_realtime(self, scheduled_day, realtime_object=None):
        """This method is used to propagate results when batch queries are
        performed by the ResultsSet, or when a single query is made.

        It will add some meta information about it.
        :param scheduled_day:
        :param realtime_object:
        """
        self._realtime_query_day = scheduled_day

        if realtime_object:
            assert isinstance(realtime_object, RealTimeDeparture)
            self.RealTime = realtime_object
            self._realtime_found = True
        else:
            self._realtime_found = False

    def perform_realtime_query(self, scheduled_day, ignore_error=True):
        """This method will perform a query to dynamo to get realtime
        information about the StopTime in this result object only.
        \nIt requires a day, because a given trip_id can be on different dates.
        :param scheduled_day:
        :param ignore_error:
        """
        assert self.has_stoptime()
        station_id, day_train_num = self.get_realtime_query_index(scheduled_day)

        # Try to get it from dynamo
        try:
            realtime_object = RealTimeDeparture.get(
                hash_key=station_id,
                range_key=day_train_num
            )
            self.set_realtime(
                scheduled_day=scheduled_day,
                realtime_object=realtime_object
            )

        except DoesNotExist:
            self.set_realtime(
                scheduled_day=scheduled_day,
                realtime_object=False
            )
            logging.info("Realtime not found for %s, %s" %
                         (station_id, day_train_num))
            if not ignore_error:
                raise DoesNotExist

    def compute_stoptime_state(self, at_datetime=None):
        """ This method will add a dictionary in the "TripPredictor" attribute.

        It will be made of:
        - at_time: the time considered
        - delay (between schedule and realtime) if realtime is found
        - passed_schedule: has train passed based on schedule information, at
        time passed as paramater (if none provided = now).
        - passed_realtime: has train passed based on realtime information.
        :param at_datetime:
        """

        assert self.has_stoptime()

        if not at_datetime:
            at_datetime = get_paris_local_datetime_now()

        self.StopTimeState = StopTimeState(
            at_datetime,
            self._scheduled_day,
            self.StopTime,
            self.RealTime if self.has_realtime() else None)


class ResultsSet:

    def __init__(self, raw_result, scheduled_day=None):
        """
        Can accept raw_result either as single element, or as list
        :param raw_result:
        :param scheduled_day: scheduled_day str format
        :return:
        """
        self.scheduled_day = scheduled_day or get_paris_local_datetime_now().strftime("%Y%m%d")

        if not isinstance(raw_result, list):
            raw_result = [raw_result]

        self.results = tuple(SingleResult(raw, self.scheduled_day) for raw in raw_result)

    def __repr__(self):
        return "<ResultsSet(scheduled_day='%s', nb_elements='%s', sample='%s')>"\
            % (self.scheduled_day, len(self.results), self.results[0])

    def __str__(self):
        return self.__repr__()

    def _index_stoptime_results(self, scheduled_day):
        """ Index elements containing a StopTime object.
        """
        self._indexed_results = {
            result.get_realtime_query_index(scheduled_day): result
            for result in self.results
            if result.has_stoptime()
        }

    def get_nested_dicts(self, realtime_only=False):
        if realtime_only:
            return [x.get_nested_dict() for x in self.results
                    if x.has_realtime()]
        else:
            return [x.get_nested_dict() for x in self.results]

    def get_flat_dicts(self, realtime_only=False):
        if realtime_only:
            return [x.get_flat_dict() for x in self.results
                    if x.has_realtime()]
        else:
            return [x.get_flat_dict() for x in self.results]

    def batch_realtime_query(self, scheduled_day=None):
        logging.info(
            "Trying to get realtime information from DynamoDB for %s items." % len(self.results))
        scheduled_day = scheduled_day or self.scheduled_day
        # 1: get all elements that have StopTime
        # 2: build all indexes (station_id, day_train_num)
        self._index_stoptime_results(scheduled_day)
        # 3: send a batch request to get elements
        # 4: dispatch correcly answers
        item_keys = [key for key, value in self._indexed_results.items()]

        i = 0
        for item in RealTimeDeparture.batch_get(item_keys):
            index = (item.station_id, item.day_train_num)
            self._indexed_results[index].set_realtime(scheduled_day, item)
            i += 1

        logging.info("Found realtime information for %s items." % i)
        # 5: SingleResult instances objects are then already updated
        # and available under self.results

    def first_with_realtime(self):
        """Mostly for debugging: returns first result which has
        realtime.
        """
        for el in self.results:
            if el.has_realtime():
                return el

    def number_of_found_realtime(self):
        """Returns number of elements that have realtime.
        """
        i = 0
        for el in self.results:
            if el.has_realtime():
                i += 1
        return i

    def compute_stoptimes_states(self, at_datetime=None):
        for res in self.results:
            res.compute_stoptime_state(at_datetime=at_datetime)

