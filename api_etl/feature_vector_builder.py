"""
Classes used to build feature vectors for predictions.

Steps:
- ask for a given Trip (on given day)
- collect Trip
- collect StopTimes
- compute StopTimeStates

"""

import logging
import statistics
from datetime import datetime, timedelta
from bdateutil import isbday

from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter
from api_etl.models import Trip, Stop
from api_etl.realtime_querier import StopTimeState, ResultsSet
from api_etl.schedule_querier import DBQuerier

logger = logging.getLogger(__name__)

__NECESSARY_FEATURES__ = {
    "predicted_station_median_delay",  # predicted station
    "last_observed_delay",  # predicted trip
    "sequence_diff",  # predicted station
    "between_stations_scheduled_trip_time",  # predicted station
    "rolling_trips_on_line",  # general
    "business_day"  # general
}


# CLASS StopTimeFeatureVector
# Create class programmatically from necessary features
# (made so to be able to create easily a serializer from class attributes)
def set_features(obj, **kwargs):
        for key, value in kwargs.items():
            setattr(obj, key, value)


def is_complete(obj):
    for feature in StopTimeFeatureVector._necessary_features:
        feature_value = getattr(obj, feature, None)
        if feature_value is None:
            return False
    return True


def __repr__(obj):
    features_dict = {
        feature_name: getattr(obj, feature_name, None)
        for feature_name in StopTimeFeatureVector._necessary_features
        }
    inner = ", ".join((map(lambda x: x+"='{"+x+"}'", features_dict.keys())))
    message = "<StopTimeFeatureVector("+inner+")>"

    return message.format(**features_dict)


def __str__(obj):
    return obj.__repr__()


class_dict = dict.fromkeys(__NECESSARY_FEATURES__, None)
class_dict.update({"_necessary_features": __NECESSARY_FEATURES__})
class_dict.update({"set_features": set_features, "is_complete": is_complete, "__repr__": __repr__, "__str__": __str__})

StopTimeFeatureVector = type("StopTimeFeatureVector", (object,), class_dict)


class StationState:
    """
    Class representing station state at a given time.

    This will be used to know what was the mean delay on a given station, to provide features for
    prediction vectors.

    Steps will be:
    - to ask for stoptimes that were scheduled for the last n minutes
    - update the scheduled stoptimes with realtime and compute stoptime states
    - compute the mean or median delay
    """
    stop = None
    at_datetime = None
    scheduled_day = None
    _stoptimes_results = None
    mean_delay = None
    median_delay = None

    def __init__(self, stop_id=None, stop=None, scheduled_day=None):

        self._dbq = DBQuerier(scheduled_day=scheduled_day)

        # ARGS PARSING
        if stop:
            assert isinstance(stop, Stop)
            self.stop = stop

        else:
            assert stop_id
            self._get_stop(stop_id)

        self.at_datetime = get_paris_local_datetime_now()

        if scheduled_day:
            datetime.strptime(scheduled_day, "%Y%m%d")
            self.scheduled_day = str(scheduled_day)
        else:
            self.scheduled_day = self.at_datetime.strftime("%Y%m%d")

        logger.debug("StationState __init__, for day {}, and stop {}"
                     .format(scheduled_day, stop.stop_name))

        # ATTRIBUTES INIT
        self._stoptimes_results = {}
        self.number_of_stops = None
        self.mean_delay = None
        self.median_delay = None
        self.number_stoptimes_schedule = None
        self.number_stoptimes_with_realtime = None

        # COMPUTATIONS
        # find stoptimes with realtime
        self._query_stoptimes()
        self._compute_delay_stats()

    def _get_stop(self, stop_id):
        logger.info("DB Query to get stop {}.".format(stop_id))
        stop = self._dbq.stations(stop_id=stop_id, level=1, limit=1)[0]
        assert isinstance(stop, Stop)
        self.stop = stop

    def _query_stoptimes(self):
        """
        Gets schedule and then realtime information.

        We want only scheduled stops from last 30 minutes (up to 5 minutes after request if some trains arrived in
        advance)

        Query params
        level=3, on_day=scheduled_day
        :return:
        """
        low_limit = (self.at_datetime - timedelta(minutes=30)).strftime("%H:%M:%S")
        high_limit = (self.at_datetime + timedelta(minutes=5)).strftime("%H:%M:%S")

        logger.info("DB Query to get stoptimes at station {}, on day {} between {} and {}"
                     .format(self.stop.stop_name, self.scheduled_day, low_limit, high_limit))

        schedule_results = self._dbq.stoptimes(
            on_day=self.scheduled_day,
            level=3,
            stop_id=self.stop.stop_id,
            departure_time_above=low_limit,
            departure_time_below=high_limit
        )
        realtime_results = ResultsSet(schedule_results, scheduled_day=self.scheduled_day)
        realtime_results.batch_realtime_query()
        realtime_results.compute_stoptimes_states(at_datetime=self.at_datetime)

        for result in realtime_results.results:
            sequence_number = int(result.StopTime.stop_sequence)
            self._stoptimes_results[sequence_number] = result

        self.number_of_stops = len(self._stoptimes_results)

    def _compute_delay_stats(self):
        """
        Find median and mean.
        :return:
        """
        self.number_stoptimes_schedule = len(self._stoptimes_results)

        delays = list(map(lambda x: x.StopTimeState.delay, self._stoptimes_results.values()))
        delays = list(filter(lambda x: not not x, delays))
        if len(delays) > 0:
            self.mean_delay = statistics.mean(delays)
            self.median_delay = statistics.median(delays)
            self.number_stoptimes_with_realtime = len(
                list(filter(lambda x: x.has_realtime(), self._stoptimes_results.values()))
            )

    def __repr__(self):
        return "<StationState(stop_id='%s', stop_name='%s', number_stoptimes_schedule='%s', " \
               "number_stoptimes_with_realtime='%s')>"\
            % (self.stop.stop_id, self.stop.stop_name, self.number_stoptimes_schedule,
               self.number_stoptimes_with_realtime)

    def __str__(self):
        return self.__repr__()


class StopTimePredictor:
    """
    For a given StopTime, we can build a vector of prediction.

    A prediction is indexed by both a StopTime (Trip x Station) and StopTimeState (at_datetime)
    (given that StopTime is already part of StopTimeState, only StopTimeState is needed).

    The prediction vector will be built only for StopTimeStates that have not passed yet (either realtime or schedule).

    The features to build are:
    - Trip previous observed delay
    - Station previous observed delay (if possible on direction)
    - working_day
    - nb of rolling trips on line
    - sequence diff (or stations_scheduled_trip_time)
    """
    at_datetime = None
    scheduled_day = None

    _Stop = None
    _StopTime = None
    _StopTimeState = None
    _RealTime = None

    next_stop_passed_realtime = None
    to_predict = None

    _predicted_station_state = None
    stoptime_feature_vector = None

    def __init__(self, stoptimestate, stop, at_datetime, scheduled_day=None):

        # ARGS PARSING
        assert isinstance(stoptimestate, StopTimeState)
        # contains StopTime and RealTime as hidden properties
        self._StopTimeState = stoptimestate
        self._RealTime = stoptimestate._RealTime
        self._StopTime = stoptimestate._StopTime

        assert isinstance(stop, Stop)
        self._Stop = stop

        assert isinstance(at_datetime, datetime)
        self.at_datetime = at_datetime

        if scheduled_day:
            datetime.strptime(scheduled_day, "%Y%m%d")
            self.scheduled_day = str(scheduled_day)
        else:
            self.scheduled_day = self.at_datetime.strftime("%Y%m%d")

        logger.debug("StopTimePredictor __init__ for stoptime of trip {} at station {} (sequence {})"
                     .format(self._StopTime.trip_id, self._Stop.stop_name, self._StopTime.stop_sequence))

        # ATTRIBUTES INIT
        # will be used in case realtime information is missing on stoptime, and that next station know that realtime
        # is passed
        self.stoptime_feature_vector = StopTimeFeatureVector()
        self.next_stop_passed_realtime = None
        self.to_predict = None
        self.last_observed_info = None
        self._predicted_station_state = None
        self.predicted_station_stats = None

        # COMPUTATIONS
        # many are triggered by TripPredictor object, since this is needed only if this StopTime is to be predicted
        # 4 main parts:
        # - information about predicted trip last stoptime
        # - information about predicted station stoptimes of last 30 minutes
        # - information about number of trips rolling at time (maybe could be interesting to add mean with terminus)
        # - information business day or not

        # at init, only doing operations that cost no query
        self._set_business_day_feature()

    def __repr__(self):
        return "<StopTimePredictor(trip_id='%s', stop_name='%s', stop_sequence='%s', departure_time='%s', " \
               "at_datetime='%s', passed_schedule='%s', has_realtime='%s', passed_realtime='%s', to_predict='%s', " \
               "predictable='%s')>"\
            % (self._StopTime.trip_id, self._Stop.stop_name, self._StopTime.stop_sequence,
               self._StopTime.departure_time, self._StopTimeState.at_datetime, self._StopTimeState.passed_schedule,
               self.has_realtime(), self.has_passed_realtime(), self.to_predict, self.is_predictable())

    def __str__(self):
        return self.__repr__()

    def has_realtime(self):
        return not not self._RealTime

    def has_passed_realtime(self):
        """
        It has passed: if next station is passed, or if this one has realtime observed
        :return:
        """
        if self.next_stop_passed_realtime:
            return True

        if self.has_realtime():
            return self._RealTime._has_passed(at_datetime=self.at_datetime)

    def set_next_stop_passed_realtime(self, next_stop_passed_realtime):
        self.next_stop_passed_realtime = next_stop_passed_realtime

    def get_last_observed_information(self):
        """
        Method used in case this particular stoptime is the last observed one for a trip sequence.
        It will then send information for next stoptimes.
        :return:
        """
        stop_sequence = self._StopTime.stop_sequence
        scheduled_departure_time = self._StopTime.departure_time
        delay = self._StopTimeState.delay

        return stop_sequence, scheduled_departure_time, delay

    def _compute_stoptimes_scheduled_diff(self, last_observed_scheduled_departure_time, last_observed_stop_sequence):
        # could use static method
        self.time_diff = DateConverter(
            normal_date=self._StopTimeState._scheduled_day,
            normal_time=self._StopTime.departure_time)\
            .compute_delay_from(
               special_date=self._StopTimeState._scheduled_day,
               special_time=last_observed_scheduled_departure_time,
        )
        self.sequence_diff = int(self._StopTime.stop_sequence) - int(last_observed_stop_sequence)

    def set_last_observed_information(self, stop_sequence, scheduled_departure_time, delay):
        """
        Needed:
        - last observed stop sequence: int
        - last observed scheduled departure-time
        - last observed delay
        :param stop_sequence:
        :param scheduled_departure_time:
        :param delay:
        :return:
        """
        self.last_observed_info = {
            "stop_sequence": stop_sequence,
            "scheduled_departure_time": scheduled_departure_time,
            "delay": delay
        }
        self._compute_stoptimes_scheduled_diff(scheduled_departure_time, stop_sequence)

        # then add to prediction features (all that are related to last observed delay)
        self.stoptime_feature_vector.set_features(
            last_observed_delay=delay,
            between_stations_scheduled_trip_time=self.time_diff,
            sequence_diff=self.sequence_diff
            )

    def label_as_to_predict(self):
        self.to_predict = True

    def get_predicted_station_stats(self):
        self._predicted_station_state = StationState(stop=self._Stop, scheduled_day=self.scheduled_day)
        self.predicted_station_stats = {
            "median_delay": self._predicted_station_state.median_delay,
            "mean_delay": self._predicted_station_state.mean_delay,
            "number_stoptimes_schedule": self._predicted_station_state.number_stoptimes_schedule,
            "number_stoptimes_with_realtime": self._predicted_station_state.number_stoptimes_with_realtime
        }
        self.stoptime_feature_vector.set_features(predicted_station_median_delay=self\
            .predicted_station_stats["median_delay"])

    def _set_business_day_feature(self):
        self.stoptime_feature_vector.set_features(business_day=isbday(self.scheduled_day))

    def is_predictable(self):
        """
        To be predictable, a stoptime vector must be to predict and have all matrix features:
        - stats about predicted station
        - last observed stoptime delay of predicted trip
        :return:
        """
        if not self.to_predict:
            return False

        return self.stoptime_feature_vector.is_complete()


class TripPredictor:
    """
    Information about a Trip State at a given time.
    - what are scheduled stoptimes, and do they have realtime observations
    - between which stations it is located?
    - how much time has passed since last stop
    - how much stops this Trip has in total (time-agnostic)
    - which stations are passed
    - which stations are yet to be passed
    - what is the last observed delay
    - is the last observed delay the last one passed on schedule? (do we have missing data)
    - what is the last observed stop scheduled stop (to know how distant is predicted station)

    This class will propagate information to stoptimes to build features vectors.

    """
    # define required fields used to build serializer
    # these class attributes will be overriden by instances attributes
    _scheduled_day = None
    trip = None  # Trip object
    _stoptime_predictors = {}  # StopTimePredictor objects, containing StopTimes, StopTimeStates, Stops
    number_of_stops = None
    # sequence number (then accessed through stops)
    last_observed_stop = None
    # sequence numbers to know which are to be predicted
    to_predict_stoptimes = set()

    def __init__(self, trip_id=None, trip=None, scheduled_day=None):

        self._dbq = DBQuerier(scheduled_day=scheduled_day)

        # ARGS PARSING
        if trip:
            assert isinstance(trip, Trip)
            self.trip = trip

        else:
            assert trip_id
            self._get_trip(trip_id)

        self.at_datetime = get_paris_local_datetime_now()

        if scheduled_day:
            datetime.strptime(scheduled_day, "%Y%m%d")
            self.scheduled_day = str(scheduled_day)
        else:
            self.scheduled_day = self.at_datetime.strftime("%Y%m%d")

        logger.info("TripPredictor __init__ for trip {} on day {}"
                     .format(self.trip.trip_id, self.scheduled_day))

        # ATTRIBUTES INIT
        self._stoptime_predictors = {}
        self.first_observed_stop = None
        self.number_of_stops = None
        self.last_observed_stop = None
        self.last_observed_passed_stop = None
        self.to_predict_stoptimes = set()

        # COMPUTATIONS
        # find stoptimes with realtime
        self._query_stoptimes()
        # find first and last observed
        self._first_observed_stoptime_index()
        self._last_observed_stoptime_index()
        # find last observed PASSED stoptime
        self._last_observed_passed_stoptime_index()
        # find out which can be predicted
        self._build_to_predict_stoptime_indexes()
        # propage last observation
        self._build_prediction_vectors_for_stoptimes_to_predict()

    def _get_trip(self, trip_id):
        logger.info("DB Query to get trip {}".format(trip_id))
        trip = self._dbq.trips(trip_id=trip_id, level=1, limit=1)[0]
        assert isinstance(trip, Trip)
        self.trip = trip

    def _query_stoptimes(self):
        # gets schedule and then realtime information
        logger.info("DB Query to get stoptimes of trip {}.".format(self.trip.trip_id))
        schedule_results = self._dbq.stoptimes(on_day=self._scheduled_day, trip_id_filter=self.trip.trip_id, level=3)
        realtime_results = ResultsSet(schedule_results, scheduled_day=self._scheduled_day)
        realtime_results.batch_realtime_query()
        realtime_results.compute_stoptimes_states(at_datetime=self.at_datetime)

        for result in realtime_results.results:
            sequence_number = int(result.StopTime.stop_sequence)
            self._stoptime_predictors[sequence_number] = StopTimePredictor(
                stoptimestate=result.StopTimeState,
                stop=result.Stop,
                at_datetime=self.at_datetime)

        self.number_of_stops = len(self._stoptime_predictors)

    def _first_observed_stoptime_index(self):
        """
        Returns first observed stoptime index
        If none, returns None

        :return:
        """
        i = 0
        while i <= self.number_of_stops-1:
            if self._stoptime_predictors[i].has_realtime():
                self.first_observed_stop = i
                return i
            i += 1

    def _last_observed_stoptime_index(self):
        """
        Returns last observed stoptime index
        If none, returns None

        :return:
        """
        i = self.number_of_stops-1
        while i >= 0:
            if self._stoptime_predictors[i].has_realtime():
                self.last_observed_stop = i
                return i
            i -= 1

    def _last_observed_passed_stoptime_index(self):
        """
        Returns last passed observed stoptime index
        If none, returns None

        :return:
        """
        i = self.number_of_stops-1
        while i >= 0:
            if self._stoptime_predictors[i].has_passed_realtime():
                self.last_observed_passed_stop = i
                return i
            i -= 1

    def _backward_propagate_passed_realtime(self):
        """
        This method is used to indicate that stoptimes are passed (realtime) if the stop after has been observed and
        is passed.
        :return:
        """
        i = self.last_observed_stop
        while i > 0:
            if self._stoptime_predictors[i].has_passed_realtime():
                self._stoptime_predictors[i].set_next_stop_passed_realtime(True)
            i -= 1

    def _compute_predictions(self):
        """

        :return:
        """
        pass

    def _build_to_predict_stoptime_indexes(self):
        """
        Conditions:
        - > first observation
        - > last passed observed realtime
        - not passed realtime

        We compute predictions, if and only if at least one stoptime is observed. And only for
        stops after the first one that has been observed!
        Then we compute predictions for all trains that have not passed realtime.

        => this means that we might make prediction for past: for example, if we missed some realtime information for
        a stop, it doesn't prevent us to predict that it arrived some time ago.

        Will fill self.to_predict set

        :return: None
        """

        if self.first_observed_stop is None:
            return None

        for i in range(self.last_observed_passed_stop+1, self.number_of_stops):
            if not self._stoptime_predictors[i].has_passed_realtime():
                self.to_predict_stoptimes.add(i)

        logger.info("There are {} stoptimes to predict (from {} to {})"
                     .format(len(self.to_predict_stoptimes),
                             min(self.to_predict_stoptimes),
                             max(self.to_predict_stoptimes))
                     )

    def _number_of_trips_rolling_at(self):
        self.trips_rolling_at_time = self\
            ._dbq.trips(on_day=self.scheduled_day, active_at_time=True, count=True)
        return self.trips_rolling_at_time

    def _build_prediction_vectors_for_stoptimes_to_predict(self):
        """
        Sends information to StopTimePredictor objects that are supposed to make predictions.
        Label them as to predict.
        Ask the ones to predict to ask for predicted station states.

        Compute number of trips rolling and send information to prediction vectors

        :return:
        """
        if self.first_observed_stop is None:
            return None

        # Last observed passed stop
        last_observed_passed_stop = self._stoptime_predictors[self.last_observed_passed_stop]
        ini_info = last_observed_passed_stop.get_last_observed_information()

        # Number of trips rolling at time
        rolling_at_time = self._number_of_trips_rolling_at()

        for i in self.to_predict_stoptimes:
            self._stoptime_predictors[i].label_as_to_predict()
            self._stoptime_predictors[i].set_last_observed_information(*ini_info)
            self._stoptime_predictors[i].get_predicted_station_stats()
            self._stoptime_predictors[i].stoptime_feature_vector.set_features(rolling_trips_on_line=rolling_at_time)

    def __repr__(self):
        return "<TripPredictor(trip_id='%s', at_datetime='%s', number_of_stops='%s', number_to_predict='%s', " \
               "number_predictable='%s')>"\
            % (self.trip.trip_id, self.at_datetime, self.number_of_stops, len(self.to_predict_stoptimes),
               sum(el.is_predictable() for el in self._stoptime_predictors.values()))

    def __str__(self):
        return self.__repr__()
