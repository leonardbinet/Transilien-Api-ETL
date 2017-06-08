"""Module containing class to build feature matrices for prediction.
"""

from os import path, makedirs
import logging

from datetime import datetime, timedelta
import numpy as np
import pandas as pd

if __name__ == '__main__':
    import logging.config
    logging.config.fileConfig('logging.conf')

from api_etl.utils_misc import (
    get_paris_local_datetime_now, DateConverter, S3Bucket
)
from api_etl.query import DBQuerier
from api_etl.serializers import ResultSetSerializer
from api_etl.settings import data_path, s3_buckets

pd.options.mode.chained_assignment = None


class DayMatrixBuilder():
    """Build features and label matrices from data available from schedule
    and from realtime info.

    1st step (init): get all information from day (schedule+realtime):
    needs day parameter (else set to today).
    2nd step: build matrices using only data available at given time: needs
    time parameter (else set to time now).

    Still "beta" functionality: provide df directly.
    """

    def __init__(self, day=None, df=None):
        """ Given a day, will query schedule and realtime information to
        provide a dataframe containing all stops.
        """

        # Arguments validation and parsing
        if day:
            # will raise error if wrong format
            datetime.strptime(day, "%Y%m%d")
            self.day = str(day)
        else:
            dt_today = get_paris_local_datetime_now()
            self.day = dt_today.strftime("%Y%m%d")

        logging.info("Day considered: %s" % self.day)

        if isinstance(df, pd.DataFrame):
            self._initial_df = df
            self._builder_realtime_request_time = None
            logging.info("Dataframe provided for day %s" % self.day)
        else:
            logging.info("Requesting data for day %s" % self.day)
            self.querier = DBQuerier(yyyymmdd=self.day)
            # Get schedule
            self.stops_results = self.querier.stoptimes_of_day(self.day)
            self.serialized_stoptimes = ResultSetSerializer(self.stops_results)
            logging.info("Schedule queried.")
            # Perform realtime queries
            dt_realtime_request = get_paris_local_datetime_now()
            self._builder_realtime_request_time = dt_realtime_request\
                .strftime("%H:%M:%S")
            self.serialized_stoptimes.batch_realtime_query(self.day)
            logging.info("RealTime queried.")
            # Export flat dict as dataframe
            self._initial_df = pd\
                .DataFrame(self.serialized_stoptimes.get_flat_dicts())
            logging.info("Initial dataframe created.")
            # Datetime considered as now
            self.paris_datetime_now = get_paris_local_datetime_now()
            self._clean_initial_df()
            logging.info("Initial dataframe cleaned.")
            self._compute_initial_dates()
            logging.info("Initial dataframe calculations computed.")

    def _clean_initial_df(self):
        """ Set Nan values, and convert necessary columns as float.
        """
        # Replace Unknown by Nan
        self._initial_df.replace("Unknown", np.nan, inplace=True)
        # Convert to numeric
        cols_to_num = ["StopTime_stop_sequence", "RealTime_data_freshness"]
        for col in cols_to_num:
            self._initial_df.loc[:, col] = pd\
                .to_numeric(self._initial_df.loc[:, col], errors="coerce")

    def _compute_initial_dates(self):
        """ Adds following columns:
        - D_business_day: bool
        - D_stop_special_day: yyyymmdd str, day in special date (25h)
        - D_total_sequence: int: number of stops scheduled per trip
        - D_stop_scheduled_datetime: datetime of scheduled stoptime
        - D_trip_passed_scheduled_stop: bool
        """
        # Detect if working day
        self._initial_df.loc[:, "D_business_day"] = bool(
            len(pd.bdate_range(self.day, self.day)))

        # Write stoptime_day
        self._initial_df.loc[:, "D_stop_special_day"] = self.day

        # Scheduled stop datetime
        # TODO: here error!! we don't have special date..
        self._initial_df.loc[:, "D_stop_scheduled_datetime"] = self._initial_df\
            .StopTime_departure_time\
            .apply(lambda x: DateConverter(
                special_time=x,
                special_date=self.day,
                force_regular_date=True
            ).dt
        )

        # Has really passed schedule
        self._initial_df.loc[:, "D_trip_passed_scheduled_stop"] = self._initial_df.D_stop_scheduled_datetime\
            .apply(lambda x:
                   (self.paris_datetime_now - x).total_seconds() >= 0
                   )

        # Observed stop datetime
        self._initial_df.loc[:, "D_stop_observed_datetime"] = self\
            ._initial_df[self._initial_df.RealTime_data_freshness.notnull()]\
            .apply(lambda x: DateConverter(
                special_time=x.RealTime_expected_passage_time,
                special_date=x.RealTime_expected_passage_day
            ).dt,
            axis=1
        )

        self._initial_df.loc[:, "D_trip_time_to_observed_stop"] = self\
            ._initial_df[self._initial_df.D_stop_observed_datetime.notnull()]\
            .D_stop_observed_datetime\
            .apply(lambda x:
                   (self.paris_datetime_now - x).total_seconds()
                   )

        # Has really passed observed stop
        self._initial_df.loc[:, "D_trip_passed_observed_stop"] = self\
            ._initial_df[self._initial_df.D_stop_observed_datetime.notnull()]\
            .D_trip_time_to_observed_stop\
            .apply(lambda x: (x >= 0))

        # Trip delay
        self._initial_df.loc[:, "D_trip_delay"] = self\
            ._initial_df[self._initial_df.RealTime_data_freshness.notnull()]\
            .apply(
                lambda x:
                    (x["D_stop_observed_datetime"] -
                     x["D_stop_scheduled_datetime"])
            .total_seconds(),
                axis=1
        )

        # Trips total number of stops
        trips_total_number_stations = self._initial_df\
            .groupby("Trip_trip_id")["Stop_stop_id"].count()
        trips_total_number_stations.name = "D_trip_number_of_stops"
        self._initial_df = self._initial_df\
            .join(trips_total_number_stations, on="Trip_trip_id")

    def stats(self):

        message = """
        SUMMARY FOR DAY %(day)s: based on information available and requested
        at time %(request_time)s, and trips passage being evaluated given time
        %(date_now)s

        TRIPS
        Number of trips today: %(trips_today)s

        STOPTIMES
        Number of stop times that day: %(stoptimes_today)s
        - Passed:
            - scheduled: %(stoptimes_passed)s
            - observed: %(stoptimes_passed_observed)s
        - Not passed yet:
            - scheduled: %(stoptimes_not_passed)s
            - observed (predictions on boards) %(stoptimes_not_passed_observed)s
        """

        self.summary = {
            "day": self.day,
            "request_time": self._builder_realtime_request_time,
            "date_now": self.paris_datetime_now,
            "trips_today": len(self._initial_df.Trip_trip_id.unique()),
            "stoptimes_today": self._initial_df.Trip_trip_id.count(),
            "stoptimes_passed": self._initial_df
            .D_trip_passed_scheduled_stop.sum(),
            "stoptimes_passed_observed": self._initial_df.
            D_trip_passed_observed_stop.sum(),
            "stoptimes_not_passed": (~self._initial_df.D_trip_passed_scheduled_stop).sum(),
            "stoptimes_not_passed_observed":
            (self._initial_df.D_trip_passed_observed_stop == False).sum(),
        }
        print(message % self.summary)

    def missing_data_per(self, per="Route_route_short_name"):
        # per can be also "Stop_stop_id", "Route_route_short_name"
        md = self._initial_df.copy()
        md.loc[:, "observed"] = md\
            .loc[:, "RealTime_day_train_num"]\
            .notnull().apply(int)

        group = md.groupby(per)["observed"]

        agg_observed = group.sum()
        agg_scheduled = group.count()
        agg_ratio = group.mean()
        agg = pd.concat([agg_observed, agg_scheduled, agg_ratio], axis=1)
        agg.columns = ["Observed", "Scheduled", "Ratio"]
        return agg


class DirectPredictionMatrix(DayMatrixBuilder):

    # CONFIGURATION
    # Number of past seconds considered for station median delay
    _secs = 1200

    # Features columns
    _feature_cols = [
        "Route_route_short_name",
        "TS_last_observed_delay",
        "TS_line_station_median_delay",
        "TS_line_median_delay",
        "Trip_direction_id",
        "TS_sequence_diff",
        "TS_stations_scheduled_trip_time",
        "TS_rolling_trips_on_line",
        "RealTime_miss",
        "D_business_day"
    ]
    # Core identification columns
    _id_cols = [
        "TS_matrix_datetime",
        "Route_route_short_name",
        "RealTime_miss",
        "Trip_trip_id",
        "Stop_stop_id",
        "TS_sequence_diff",
        "TS_stations_scheduled_trip_time",
    ]
    # Label columns
    _label_cols = ["label", "label_ev"]

    # Scoring columns
    _scoring_cols = ["S_naive_pred_mae", "S_naive_pred_mse"]

    # Prediction columns
    _prediction_cols = ["P_api_pred", "P_api_pred_ev", "P_naive_pred"]

    # Other useful columns
    _other_useful_cols = [
        "StopTime_departure_time",
        "StopTime_stop_sequence",
        "Stop_stop_name",
        "RealTime_expected_passage_time",
        "RealTime_data_freshness",
    ]

    # For time debugging:
    _time_debug_cols = [
        "StopTime_departure_time", "RealTime_expected_passage_time",
        'D_stop_special_day', 'D_stop_scheduled_datetime',
        'D_trip_passed_scheduled_stop', 'D_stop_observed_datetime',
        'D_trip_time_to_observed_stop', 'D_trip_passed_observed_stop',
        'D_trip_delay', 'TS_matrix_datetime',
        'TS_trip_passed_scheduled_stop', 'TS_observed_vs_matrix_datetime',
        'TS_trip_passed_observed_stop', 'TS_observed_delay',
        'TS_expected_delay', 'TS_trip_status'
    ]

    def __init__(self, day=None, df=None):
        DayMatrixBuilder.__init__(self, day=day, df=df)
        self._state_at_time_computed = False

    def direct_compute_for_time(self, time="12:00:00"):
        """Given the data obtained from schedule and realtime, this method will
        compute network state at a given time, and provide prediction and label
        matrices.
        """

        # Parameters parsing
        full_str_dt = "%s%s" % (self.day, time)
        # will raise error if wrong format
        self.state_at_datetime = datetime\
            .strptime(full_str_dt, "%Y%m%d%H:%M:%S")
        self.time = time

        logging.info(
            "Building Matrix for day %s and time %s" % (
                self.day, self.time)
        )

        # Recreate dataframe from initial one (deletes changes)
        self.df = self._initial_df.copy()

        # Computing
        self._compute_trip_state()
        logging.info("TripState computed.")
        self._trip_level()
        logging.info("Trip level computations performed.")
        self._line_level()
        logging.info("Line level computations performed.")
        # Will add labels if information is available
        self._compute_labels()
        logging.info("Labels assigned.")
        self._compute_api_pred()
        logging.info("Api and naive predictions assigned.")
        self._compute_pred_scores()
        logging.info("Naive predictions scored.")

    def _compute_trip_state(self):
        """Computes:
        - TS_matrix_datetime: datetime
            = datetime for which state is computed
        - TS_trip_passed_scheduled_stop: Bool
            = at matrix datetime, has train passed scheduled stop?
        - TS_observed_vs_matrix_datetime: int (seconds)
        - TS_trip_passed_observed_stop: Bool
            = at matrix datetime, has train passed observed stop?
        - TS_observed_delay: int (seconds)
        - TS_expected_delay: int (seconds)
        """

        self.df.loc[:, "TS_matrix_datetime"] = self.state_at_datetime\
            .strftime("%Y%m%d-%H:%M:%S")

        # Has passed scheduled stop at state datetime
        self.df.loc[:, "TS_trip_passed_scheduled_stop"] = self.df\
            .D_stop_scheduled_datetime\
            .apply(lambda x:
                   ((self.state_at_datetime - x).total_seconds() >= 0),
                   )

        # Time between matrix datetime (for which we compute the prediction
        # features matrix), and stop times observed passages (only for observed
        # passages). <0 means passed, >0 means not passed yet at the given time
        self.df.loc[:, "TS_observed_vs_matrix_datetime"] = self\
            .df[self.df["D_stop_observed_datetime"].notnull()]\
            .D_stop_observed_datetime\
            .apply(lambda x: (self.state_at_datetime - x).total_seconds())

        # Has passed observed stop time at state datetime
        self.df.loc[:, "TS_trip_passed_observed_stop"] = self\
            .df[self.df["TS_observed_vs_matrix_datetime"]
                .notnull()]\
            .loc[:, "TS_observed_vs_matrix_datetime"]\
            .apply(lambda x: (x >= 0))

        # TripState_observed_delay
        self.df.loc[:, "TS_observed_delay"] = self\
            .df[self.df["TS_trip_passed_observed_stop"] == True]\
            .D_trip_delay

        # TripState_expected_delay
        self.df.loc[:, "TS_expected_delay"] = self\
            .df.query("(TS_trip_passed_observed_stop != True) & (RealTime_data_freshness.notnull())")\
            .D_trip_delay

    def _trip_level(self):
        """Compute trip level information:
        - TS_trip_status: 0<=x<=1: proportion of passed stations at time
        - D_total_sequence: number of stops scheduled for this trip
        - last_sequence_number: last observed stop sequence for this trip at
        time
        - last_observed_delay
        """
        # Trips total number of stops
        trips_total_number_stations = self.df\
            .groupby("Trip_trip_id")["Stop_stop_id"].count()
        # already added to day matrix

        # Trips status at time
        trips_number_passed_stations = self.df\
            .groupby("Trip_trip_id")["TS_trip_passed_scheduled_stop"].sum()
        trips_status = trips_number_passed_stations \
            / trips_total_number_stations
        trips_status.name = "TS_trip_status"
        self.trips_status = trips_status
        self.df = self.df.join(trips_status, on="Trip_trip_id")

        # Trips last observed stop_sequence
        last_sequence_number = self\
            .df.query("(TS_trip_status < 1) & (TS_trip_status > 0) & (TS_trip_passed_observed_stop == True)")\
            .groupby("Trip_trip_id")["StopTime_stop_sequence"].max()
        last_sequence_number.name = "TS_last_sequence_number"
        self.df = self.df.join(last_sequence_number, on="Trip_trip_id")

        # Compute number of stops between last observed station and predicted
        # station.
        self.df.loc[:, "TS_sequence_diff"] = self.df.StopTime_stop_sequence - \
            self.df.loc[:, "TS_last_sequence_number"]

        # Trips last observed delay
        last_observed_delay = self.df\
            .query("TS_last_sequence_number==StopTime_stop_sequence")\
            .loc[:, ["Trip_trip_id", "TS_observed_delay"]]
        last_observed_delay.set_index("Trip_trip_id", inplace=True)
        last_observed_delay.columns = ["TS_last_observed_delay"]
        self.df = self.df.join(last_observed_delay, on="Trip_trip_id")

        # Trips last observed scheduled departure time
        # useful to know how much time was scheduled between stations
        last_observed_scheduled_dep_time = self.df\
            .query("TS_last_sequence_number==StopTime_stop_sequence")\
            .loc[:, ["Trip_trip_id", "StopTime_departure_time"]]
        last_observed_scheduled_dep_time\
            .set_index("Trip_trip_id", inplace=True)
        last_observed_scheduled_dep_time.columns = [
            "TS_last_observed_scheduled_dep_time"]
        self.df = self.df\
            .join(last_observed_scheduled_dep_time, on="Trip_trip_id")

        # Compute number of seconds between last observed passed trip scheduled
        # departure time, and departure time of predited station
        self.df.loc[:, "TS_stations_scheduled_trip_time"] = self.df\
            .query("TS_last_observed_scheduled_dep_time.notnull()")\
            .apply(lambda x:
                   DateConverter(dt=x["D_stop_scheduled_datetime"])
                   .compute_delay_from(
                       special_date=self.day,
                       special_time=x["TS_last_observed_scheduled_dep_time"],
                       force_regular_date=True
                   ),
                   axis=1
                   )

    def _line_level(self):
        """ Computes line level information:
        - median delay on line on last n seconds
        - median delay on line station on last n seconds
        - number of currently rolling trips on line

        Requires time to now (_add_time_to_now_col).
        """
        # Compute delays on last n seconds (defined in init self._secs)

        # Line aggregation
        line_median_delay = self.df\
            .query("(TS_observed_vs_matrix_datetime<%s) & (TS_observed_vs_matrix_datetime>=0) " % self._secs)\
            .groupby("Route_route_short_name")\
            .TS_observed_delay.median()
        line_median_delay.name = "TS_line_median_delay"
        self.df = self.df\
            .join(line_median_delay, on="Route_route_short_name")
        self.line_median_delay = line_median_delay

        # Line and station aggregation
        # same station can have different values given on which lines it
        # is located.
        line_station_median_delay = self.df\
            .query("(TS_observed_vs_matrix_datetime < %s) & (TS_observed_vs_matrix_datetime>=0) " % self._secs)\
            .groupby(["Route_route_short_name", "Stop_stop_id"])\
            .TS_observed_delay.median()
        line_station_median_delay.name = "TS_line_station_median_delay"
        self.df = self.df\
            .join(line_station_median_delay, on=["Route_route_short_name", "Stop_stop_id"])
        self.line_station_median_delay = line_station_median_delay

        # Number of currently rolling trips
        rolling_trips_on_line = self\
            .df.query("TS_trip_status>0 & TS_trip_status<1")\
            .groupby("Route_route_short_name")\
            .Trip_trip_id\
            .count()
        rolling_trips_on_line.name = "TS_rolling_trips_on_line"
        self.df = self.df\
            .join(rolling_trips_on_line, on="Route_route_short_name")
        self.rolling_trips_on_line = rolling_trips_on_line

    def _compute_labels(self):
        """Two main logics:
        - either retroactive: then TripState_expected_delay is real one: label.
        - either realtime (not retroactive): then we don't have real label, but
        we have a api prediction.

        Retroactive:
        Adds two columns:
        - label: observed delay at stop: real one.
        - label_ev: observed delay evolution (difference between observed
        delay predicted stop, and delay at last observed stop)

        Not retroactive: realtime:
        Adds two columns:
        - P_api_pred: predicted delay from api.
        - P_api_pred_ev: predicted evolution (from api) of delay.
        """
        # if stop time really occured, then expected delay (extracted from api)
        # is real one
        self.df.loc[:, "label"] = self.df\
            .query("D_trip_passed_observed_stop==True")\
            .TS_expected_delay

        # Evolution of delay between last observed station and predicted
        # station
        self.df.loc[:, "label_ev"] = self.df\
            .query("D_trip_passed_observed_stop == True")\
            .apply(lambda x: x.label - x["TS_last_observed_delay"], axis=1)

    def _compute_api_pred(self):
        """This method provides two predictions if possible:
        - naive pred: delay translation (last observed delay)
        - api prediction
        """
        # if not passed: it is the api-prediction
        self.df.loc[:, "P_api_pred"] = self.df\
            .query("D_trip_passed_observed_stop != True")\
            .TS_expected_delay
        # api delay evolution prediction
        self.df.loc[:, "P_api_pred_ev"] = self.df\
            .query("D_trip_passed_observed_stop != True")\
            .apply(lambda x: x.label - x["TS_last_observed_delay"], axis=1)

        self.df.loc[:, "P_naive_pred"] = self.df.loc[
            :, "TS_last_observed_delay"]

    def _compute_pred_scores(self):
        """
        We can compute score only for stoptimes for which we have real
        information.
        At no point we will be able to have both real information and api pred,
        so we only compute score for naive prediction.

        NAIVE PREDICTION:
        Naive prediction assumes that delay does not evolve:
        - evolution of delay = 0
        - delay predicted = last_observed_delay
        => error = real_delay - naive_pred
                 = label - last_observed_delay
                 = label_ev

        Scores for navie prediction for delay can be:
        - naive_pred_mae: mean absolute error: |label_ev|
        - naive_pred_mse: mean square error: (label_ev)**2
        """
        self.df.loc[:, "S_naive_pred_mae"] = self.df["label_ev"].abs()
        self.df.loc[:, "S_naive_pred_mse"] = self.df["label_ev"]**2

    def stats(self):

        DayMatrixBuilder.stats(self)

        if not self._state_at_time_computed:
            return None

        message = """
        SUMMARY FOR DAY %(day)s AT TIME %(time)s

        TRIPS
        Number of trips today: %(trips_today)s
        Number of trips currently rolling: %(trips_now)s (these are the trips for which we will try to make predictions)
        Number of trips currently rolling for wich we observed at least one stop: %(trips_now_observed)s

        STOPTIMES
        Number of stop times that day: %(stoptimes_today)s
        - Passed:
            - scheduled: %(stoptimes_passed)s
            - observed: %(stoptimes_passed_observed)s
        - Not passed yet:
            - scheduled: %(stoptimes_not_passed)s
            - observed (predictions on boards) %(stoptimes_not_passed_observed)s

        STOPTIMES FOR ROLLING TRIPS
        Total number of stops for rolling trips: %(stoptimes_now)s
        - Passed: those we will use to make our prediction
            - scheduled: %(stoptimes_now_passed)s
            - observed: %(stoptimes_now_passed_observed)s
        - Not passed yet: those for which we want to make a prediction
            - scheduled: %(stoptimes_now_not_passed)s
            - already observed on boards (prediction): %(stoptimes_now_not_passed_observed)s

        PREDICTIONS
        Number of stop times for which we want to make a prediction (not passed yet): %(stoptimes_now_not_passed)s
        Number of trips currently rolling for wich we observed at least one stop: %(trips_now_observed)s
        Representing %(stoptimes_predictable)s stop times for which we can provide a prediction.

        LABELED
        Given that retroactive is %(retroactive)s, we have %(stoptimes_predictable_labeled)s labeled predictable stoptimes for training.
        """

        self.summary = {
            "day": self.day,
            "time": self.time,
            "trips_today": len(self.df.Trip_trip_id.unique()),
            "trips_now": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1)")
            .Trip_trip_id.unique().shape[0],
            "trips_now_observed": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1) & (TS_sequence_diff.notnull())")
            .Trip_trip_id.unique().shape[0],
            "stoptimes_today": self.df.Trip_trip_id.count(),
            "stoptimes_passed": self.df.TS_trip_passed_scheduled_stop.sum(),
            "stoptimes_passed_observed": self
            .df.TS_trip_passed_observed_stop.sum(),
            "stoptimes_not_passed": (~self.df.TS_trip_passed_scheduled_stop).sum(),
            "stoptimes_not_passed_observed":
            (self.df.TS_trip_passed_observed_stop == False).sum(),
            "stoptimes_now": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1)")
            .Trip_trip_id.count(),
            "stoptimes_now_passed": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1) &(TS_trip_passed_scheduled_stop==True)")
            .Trip_trip_id.count(),
            "stoptimes_now_passed_observed": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1) &(TS_trip_passed_observed_stop==True)")
            .Trip_trip_id.count(),
            "stoptimes_now_not_passed": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1) &(TS_trip_passed_scheduled_stop==False)")
            .Trip_trip_id.count(),
            "stoptimes_now_not_passed_observed": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1) &(TS_trip_passed_observed_stop==False)")
            .Trip_trip_id.count(),
            "stoptimes_predictable": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1) &(TS_trip_passed_scheduled_stop==False) & (TS_sequence_diff.notnull())")
            .Trip_trip_id.count(),
            "stoptimes_predictable_labeled": self.df
            .query("(TS_trip_status > 0) & (TS_trip_status < 1) &(TS_trip_passed_scheduled_stop==False) & (TS_sequence_diff.notnull()) &(label.notnull())")
            .Trip_trip_id.count(),
        }
        print(message % self.summary)

    def get_predictable(self, all_features_required=True, labeled_only=True, col_filter_level=2, split_datasets=False, set_index=True, provided_df=None):
        """Return predictable stop times.
        """
        assert self._state_at_time_computed

        if isinstance(provided_df, pd.DataFrame):
            rdf = provided_df
        else:
            rdf = self.df

        # Filter running trips, stations not passed yet
        # Basic Conditions:
        # - trip_status stricly between 0 and 1,
        # - has not passed yet schedule (not True)
        # - has not passed yet realtime (not True, it can be Nan or False)
        rdf = rdf.query(
            "TS_trip_status < 1 & TS_trip_status > 0 & TS_trip_passed_scheduled_stop !=\
            True & TS_trip_passed_observed_stop != True")

        if all_features_required:
            # Only elements that have all features
            for feature in self._feature_cols:
                rdf = rdf.query("%s.notnull()" % feature)

        if labeled_only:
            rdf = rdf.query("label.notnull()")

        if set_index:
            rdf = self._df_set_index(rdf)

        if col_filter_level:
            # no filter, all columns
            rdf = self._df_filter_cols(rdf, col_filter_level=col_filter_level)

        if split_datasets:
            # return dict
            rdf = self._split_datasets(rdf)

        logging.info("Predictable with labeled_only=%s, has a total of %s rows." % (labeled_only, len(rdf))
                     )
        return rdf

    def _df_filter_cols(self, rdf, col_filter_level):
        # We need at least: index, features, and label
        filtered_cols = self._feature_cols\
            + self._id_cols\
            + self._label_cols\
            + self._prediction_cols\
            + self._scoring_cols

        if col_filter_level == 2:
            # high filter: only necessary fields
            return rdf[filtered_cols]

        elif col_filter_level == 1:
            # medium filter: add some useful cols
            filtered_cols += self._other_useful_cols
            return rdf[filtered_cols]

        else:
            raise ValueError("col_filter_level must be 0, 1 or 2")

    def _df_set_index(self, rdf):
        # copy columns so that it is available as value or index
        # value columns are then filtered
        assert isinstance(rdf, pd.DataFrame)
        index_suffix = "_ix"
        rdf.reset_index()
        for col in self._id_cols:
            rdf[col + index_suffix] = rdf[col]
        new_ix = list(map(lambda x: x + index_suffix, self._id_cols))
        rdf.set_index(new_ix, inplace=True)
        return rdf

    def _split_datasets(self, rdf):
        res = {
            "X": rdf[self._feature_cols],
            "y_real": rdf[self._label_cols],
            "y_pred": rdf[self._prediction_cols],
            "y_score": rdf[self._scoring_cols]
        }
        return res

    def compute_multiple_times_of_day(self, begin="00:00:00", end="23:59:00", min_diff=60, flush_former=True, **kwargs):
        """Compute dataframes for different times of day.
        Default: begins at 00:00:00 and ends at 23:59:00 with a step of one
        hour.
        """
        assert isinstance(min_diff, int)
        diff = timedelta(minutes=min_diff)

        # will raise error if wrong format
        begin_dt = datetime.strptime(begin, "%H:%M:%S")
        end_dt = datetime.strptime(end, "%H:%M:%S")

        if flush_former:
            self._flush_result_concat()

        step_dt = begin_dt
        while (end_dt >= step_dt):
            step = step_dt.strftime("%H:%M:%S")
            self.direct_compute_for_time(step)
            step_df = self.get_predictable(**kwargs)
            self._concat_dataframes(step_df)

            step_dt += diff
        return self.result_concat

    def _concat_dataframes(self, df):
        assert isinstance(df, pd.DataFrame)
        # if no former result df, create empty df
        if not hasattr(self, "result_concat"):
            self.result_concat = pd.DataFrame()

        # concat with previous results
        self.result_concat = pd.concat([self.result_concat, df])

    def _flush_result_concat(self):
        self.result_concat = pd.DataFrame()


class RecursivePredictionMatrix(DayMatrixBuilder):

    def __init__(self, day=None, df=None):
        DayMatrixBuilder.__init__(self, day=day, df=df)

    def compute_all_possibles_sets(self):
        """Given the data obtained from schedule and realtime, this method will
        compute data sets for recursive prediction.

        Recursive predictions (to the contrary of direct predictions) are
        relatively time agnostic. They primarily depend on previous stops.

        The elements to be computed are:
        - R_trip_previous_station_delay: the train previous stop delay:
            -- will only accept previous stop
        - R_previous_trip_last_station_delay: the forward train last estimated stop delay: difficult to compute?
            -- RS_data_freshness
        - R_: make a score of route section blocking potential
        """
        self.df = self._initial_df.copy()

        self._trip_previous_station()

    def _trip_previous_station(self):
        self.df.loc[:, "R_previous_station_sequence"] = self.df\
            .query("StopTime_stop_sequence>0")\
            .StopTime_stop_sequence - 1

        previous_station = self.df\
            .set_index(["Trip_trip_id", "StopTime_stop_sequence"])\
            .loc[:, ["D_trip_delay", "StopTime_departure_time"]]\
            .dropna()

        self.df = self.df\
            .join(
                previous_station,
                on=["Trip_trip_id", "R_previous_station_sequence"],
                how="left", rsuffix="_previous_station"
            )

    def _route_section_blocking_potential(self):
        """ First: find previous trip.
        Supposing multiple
        """
        pass

    def _route_section_traffic(self):
        pass


class TripVizBuilder(DayMatrixBuilder):

    def __init__(self, day=None, df=None):
        DayMatrixBuilder.__init__(self, day=day, df=df)

    def annote_for_route_section(self, passes_by_all=None, passes_by_one=None):
        """
        Adds a column: stop custom sequence. It represents the station sequence
        on the given route, on the given section.

        Trip directions will be separated.

        Filters trips passing by chosen stations.

        To compute custom route sequence, we have to assign to each relevant
        stop_id a sequence number.

        Ideally we would like to select by stop_name, but they are not unique.
        """
        pass


class TrainingSetBuilder():

    def __init__(self, start, end, tempo=60):
        dti = pd.date_range(start=start, end=end, freq="D")
        self.days = dti.map(lambda x: x.strftime("%Y%m%d")).tolist()

        assert isinstance(tempo, int)
        self.tempo = tempo

        self.bucket_name = s3_buckets["training-sets"]

        self._bucket_provider = S3Bucket(
            self.bucket_name,
            create_if_absent=True
        )

    def _create_day_training_set(self, day, save_s3, save_in):
        mat = DirectPredictionMatrix(day)
        mat.compute_multiple_times_of_day(min_diff=self.tempo)

        raw_folder_path = path.join(save_in, "raw_days")
        train_folder_path = path.join(save_in, "training_set-tempo-%s-min" %
                                      self.tempo)

        if not path.exists(raw_folder_path):
            makedirs(raw_folder_path)

        if not path.exists(train_folder_path):
            makedirs(train_folder_path)

        raw_file_path = path.join(raw_folder_path, "raw_%s.pickle" % day)
        pred_file_path = path.join(train_folder_path, "%s.pickle" % day)

        logging.info("Saving data in %s." % raw_folder_path)
        mat._initial_df.to_pickle(raw_file_path)
        mat.result_concat.to_pickle(pred_file_path)

        if save_s3:
            self._bucket_provider.send_file(file_path=raw_file_path)
            self._bucket_provider.send_file(file_path=pred_file_path)

    def create_training_sets(self, save_in=None, save_s3=True):
        save_in = save_in or path.relpath(data_path)

        for day in self.days:
            self._create_day_training_set(
                day=day,
                save_s3=save_s3,
                save_in=save_in)


# TODO
# A - take into account trip direction when computing delays on line
# DONE: B - handle cases where no realtime information is found
# DONE: C - when retroactive is False, give api prediction as feature
# D - ability to save results
# E - investigate missing values/stations
# F - perform trip_id train_num comparison on RATP lines
# G - improve speed by having an option to make computations only on running
# trains
