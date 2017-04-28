"""Module containing class to build feature matrices for prediction.
"""
import logging
import logging.config

from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter
from api_etl.query import DBQuerier

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('root')

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

    # CONFIGURATION
    # Number of past seconds considered for station median delay
    secs = 1200
    # For time debugging:
    _time_debug_cols = [
        "TripState_passed_realtime", "TripState_passed_schedule",
        "TripState_real_passage_vs_prediction_time_diff",
        "StopTime_departure_time", "RealTime_expected_passage_time"
    ]
    # Features columns
    _feature_cols = [
        "Route_route_short_name",
        "last_observed_delay",
        "line_station_median_delay",
        "line_median_delay",
        "Trip_direction_id",
        "sequence_diff",
        "stations_scheduled_trip_time",
        "rolling_trips_on_line",
        "stoptime_scheduled_hour",
        "RealTime_miss",
        "business_day"
    ]
    # Core identification columns
    _id_cols = [
        "TripState_at_datetime",
        "Route_route_short_name",
        "RealTime_miss",
        "Trip_trip_id",
        "Stop_stop_id",
        "sequence_diff",
        "stations_scheduled_trip_time",
    ]
    # Label columns
    _label_cols = ["label", "label_ev"]

    # Scoring columns
    _scoring_cols = ["naive_pred_mae", "naive_pred_mse"]

    # Other useful columns
    _other_useful_cols = [
        "StopTime_departure_time",
        "StopTime_stop_sequence",
        "Stop_stop_name",
        "RealTime_expected_passage_time",
        "RealTime_data_freshness",
    ]

    def __init__(self, day=None, df=None):
        """ Given a day, will query schedule and realtime information to
        provide a dataframe containing all stops.
        """

        self._realtime_computed = False
        # Arguments validation and parsing
        if day:
            # will raise error if wrong format
            datetime.strptime(day, "%Y%m%d")
            self.day = str(day)
        else:
            dt_today = get_paris_local_datetime_now()
            self.day = dt_today.strftime("%Y%m%d")

        logger.info("Day considered: %s" % self.day)

        if isinstance(df, pd.DataFrame):
            self._initial_df = df
            logger.info("Dataframe provided for day %s" % self.day)
        else:
            logger.info("Requesting data for day %s" % self.day)
            self.querier = DBQuerier(yyyymmdd=self.day)
            # Get schedule
            self.stops_results = self.querier.stops_of_day(self.day)
            logger.info("Schedule queried.")
            # Perform realtime queries
            self.stops_results.batch_realtime_query(self.day)
            logger.info("RealTime queried.")
            # Export flat dict as dataframe
            self._initial_df = pd\
                .DataFrame(self.stops_results.get_flat_dicts())
            logger.info("Initial dataframe created.")

    def compute_for_time(self, time=None):
        """Given the data obtained from schedule and realtime, this method will
        compute network state at a given time, and provide prediction and label
        matrices.
        """

        # Parameters parsing
        if time:
            full_str_dt = "%s%s" % (self.day, time)
            # will raise error if wrong format
            self.datetime = datetime.strptime(full_str_dt, "%Y%m%d%H:%M:%S")
            self.time = time
            self.retro_active = True

        else:
            # Keeps day, but provide time now
            dt_now = get_paris_local_datetime_now()
            self.time = dt_now.strftime("%H:%M:%S")
            full_str_dt = "%s%s" % (self.day, self.time)
            self.datetime = datetime.strptime(full_str_dt, "%Y%m%d%H:%M:%S")
            self.retro_active = False

        if self.retro_active:
            self.paris_datetime_now = get_paris_local_datetime_now()
        else:
            self.paris_datetime_now = self.datetime

        logger.info("Building Matrix for day %s and time %s (retroactive: %s)" % (
            self.day, self.time, self.retro_active))

        # Recreate dataframe from initial one (deletes changes)
        self.df = self._initial_df.copy()

        # Computing
        self._clean_initial_df()
        logger.info("Initial dataframe cleaned.")
        self._compute_trip_state()
        logger.info("TripState computed.")
        self._trip_level()
        logger.info("Trip level computations performed.")
        self._line_level()
        logger.info("Line level computations performed.")
        # Will add labels if information is available
        self._compute_labels()
        logger.info("Labels assigned.")
        self._compute_pred_scores()
        logger.info("Naive predictions scored.")
        self._realtime_computed = True

    def _clean_initial_df(self):
        # Replace Unknown by Nan
        self.df.replace("Unknown", np.nan, inplace=True)
        # Convert to numeric
        cols_to_num = ["StopTime_stop_sequence", "RealTime_data_freshness"]
        for col in cols_to_num:
            self.df.loc[:, col] = pd\
                .to_numeric(self.df.loc[:, col], errors="coerce")
        # Detect stoptime hour
        self.df.loc[:, "stoptime_scheduled_hour"] = self.df\
            .StopTime_departure_time\
            .apply(lambda x: DateConverter(
                special_time=x,
                special_date=self.day
            ).dt.hour
        )
        # Detect if working day
        self.df["business_day"] = bool(len(pd.bdate_range(self.day, self.day)))

    def _compute_trip_state(self):
        """Computes:
        - TripState_at_datetime: datetime
        - TripState_passed_schedule: Bool
        - TripState_real_passage_vs_prediction_time_diff: int (seconds)
        - TripState_passed_realtime: Bool
        - TripState_observed_delay: int (seconds)
        - TripState_expected_delay: int (seconds)
        """

        self.df.loc[:, "TripState_at_datetime"] = self.datetime\
            .strftime("%Y%m%d-%H:%M:%S")

        self.df.loc[:, "TripState_passed_schedule"] = self.df\
            .apply(lambda x: DateConverter(
                dt=self.datetime
            )
                .compute_delay_from(
                    special_date=self.day,
                    special_time=x["StopTime_departure_time"]
            ),
                axis=1
        ).apply(lambda x: (x >= 0))

        # Time between observed datetime (for which we compute the prediction
        # features matrix), and stop times observed passages (only for observed
        # passages). <0 means passed, >0 means not passed yet at the given time
        self.df.loc[:, "TripState_real_passage_vs_prediction_time_diff"] = self\
            .df[self.df.RealTime_expected_passage_time.notnull()]\
            .apply(lambda x: DateConverter(
                dt=self.datetime
            )
                .compute_delay_from(
                special_date=x["RealTime_expected_passage_day"],
                special_time=x["RealTime_expected_passage_time"]
            ),
                axis=1
        )

        self.df.loc[:, "TripState_passed_realtime"] = self\
            .df[self.df.TripState_real_passage_vs_prediction_time_diff
                .notnull()]\
            .TripState_real_passage_vs_prediction_time_diff\
            .apply(lambda x: (x >= 0))

        # TripState_observed_delay
        self.df.loc[:, "TripState_observed_delay"] = self\
            .df[self.df.TripState_passed_realtime == True]\
            .apply(
                lambda x: DateConverter(
                    special_date=x["RealTime_expected_passage_day"],
                    special_time=x["RealTime_expected_passage_time"]
                )
                .compute_delay_from(
                    special_date=self.day,
                    special_time=x["StopTime_departure_time"]
                ),
                axis=1
        )

        # TripState_expected_delay
        self.df.loc[:, "TripState_expected_delay"] = self\
            .df.query("(TripState_passed_realtime != True) & (RealTime_expected_passage_time.notnull())")\
            .apply(
                lambda x: DateConverter(
                    special_date=x["RealTime_expected_passage_day"],
                    special_time=x["RealTime_expected_passage_time"]
                )
                .compute_delay_from(
                    special_date=self.day,
                    special_time=x["StopTime_departure_time"]
                ),
                axis=1
        )

    def _trip_level(self):
        """Compute trip level information:
        - trip_status: 0<=x<=1: proportion of passed stations at time
        - total_sequence: number of stops scheduled for this trip
        - last_sequence_number: last observed stop sequence for this trip at
        time
        - last_observed_delay
        """
        # Trips total number of stops
        trips_total_number_stations = self.df\
            .groupby("Trip_trip_id")["TripState_passed_schedule"].count()
        trips_total_number_stations.name = "total_sequence"
        self.df = self.df.join(trips_total_number_stations, on="Trip_trip_id")

        # Trips status at time
        trips_number_passed_stations = self.df\
            .groupby("Trip_trip_id")["TripState_passed_schedule"].sum()
        trips_status = trips_number_passed_stations \
            / trips_total_number_stations
        trips_status.name = "trip_status"
        self.trips_status = trips_status
        self.df = self.df.join(trips_status, on="Trip_trip_id")

        # Trips last observed stop_sequence
        self.last_sequence_number = self\
            .df[(self.df.trip_status < 1) & (self.df.trip_status > 0) & (self.df.TripState_passed_realtime == True)]\
            .groupby("Trip_trip_id")["StopTime_stop_sequence"].max()
        self.last_sequence_number.name = "last_sequence_number"
        self.df = self.df.join(self.last_sequence_number, on="Trip_trip_id")

        # Compute number of stops between last observed station and predicted
        # station.
        self.df.loc[:, "sequence_diff"] = self.df.StopTime_stop_sequence - \
            self.df.last_sequence_number

        # Trips last observed delay
        self.last_observed_delay = self.df[self.df.last_sequence_number == self.df.StopTime_stop_sequence][
            ["Trip_trip_id", "TripState_observed_delay"]]
        self.last_observed_delay.set_index("Trip_trip_id", inplace=True)
        self.last_observed_delay.columns = ["last_observed_delay"]
        self.df = self.df.join(self.last_observed_delay, on="Trip_trip_id")

        # Trips last observed scheduled departure time
        # useful to know how much time was scheduled between stations
        self.last_observed_scheduled_dep_time = self\
            .df[self.df.last_sequence_number ==
                self.df.StopTime_stop_sequence][
                ["Trip_trip_id", "StopTime_departure_time"]]
        self.last_observed_scheduled_dep_time\
            .set_index("Trip_trip_id", inplace=True)
        self.last_observed_scheduled_dep_time.columns = [
            "last_observed_scheduled_dep_time"]
        self.df = self.df\
            .join(self.last_observed_scheduled_dep_time, on="Trip_trip_id")

        # Compute number of seconds between last observed passed trip scheduled
        # departure time, and departure time of predited station
        self.df.loc[:, "stations_scheduled_trip_time"] = self\
            .df[self.df.last_observed_scheduled_dep_time.notnull()]\
            .apply(lambda x: DateConverter(
                special_date=self.day,
                special_time=x["StopTime_departure_time"]
            )
                .compute_delay_from(
                    special_date=self.day,
                    special_time=x["last_observed_scheduled_dep_time"]
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
        # Compute delays on last n seconds (defined in init self.secs)

        # Line aggregation
        self.line_median_delay = self\
            .df[(self.df.TripState_real_passage_vs_prediction_time_diff < self.secs) & (self.df.TripState_real_passage_vs_prediction_time_diff >= 0)]\
            .groupby("Route_route_short_name").TripState_observed_delay.median()
        self.line_median_delay.name = "line_median_delay"
        self.df = self.df.join(
            self.line_median_delay,
            on="Route_route_short_name")

        # Line and station aggregation
        # same station can have different values given on which lines it
        # is located.
        self.line_station_median_delay = self\
            .df[(self.df.TripState_real_passage_vs_prediction_time_diff < self.secs) &
                self.df.TripState_real_passage_vs_prediction_time_diff >= 0]\
            .groupby(["Route_route_short_name", "Stop_stop_id"])\
            .TripState_observed_delay.median()
        self.line_station_median_delay.name = "line_station_median_delay"
        self.df = self.df.join(
            self.line_station_median_delay,
            on=["Route_route_short_name", "Stop_stop_id"])

        # Number of currently rolling trips
        self.rolling_trips_on_line = self\
            .df.query("trip_status>0 & trip_status<1")\
            .groupby("Route_route_short_name")["Trip_trip_id"]\
            .count()
        self.rolling_trips_on_line.name = "rolling_trips_on_line"
        self.df = self.df.join(
            self.rolling_trips_on_line,
            on="Route_route_short_name")

    def _compute_labels(self):
        """Adds two columns:
        - label: observed delay at stop
        - label_ev: observed delay evolution (difference between observed
        delay predicted stop, and delay at last observed stop)
        """
        # adds labels if information of passage is available now (the real now)
        self.df.loc[:, "_time_to_now"] = self\
            .df[self.df.RealTime_expected_passage_time.notnull()]\
            .apply(lambda x: DateConverter(
                dt=self.paris_datetime_now
            )
                .compute_delay_from(
                special_date=x["RealTime_expected_passage_day"],
                special_time=x["RealTime_expected_passage_time"]
            ),
                axis=1
        )
        self.df.loc[:, "_really_passed_now"] = self\
            .df[self.df._time_to_now.notnull()]\
            ._time_to_now.apply(lambda x: (x >= 0))

        # if stop time really occured, then expected delay (extracted from api)
        # is real one
        self.df.loc[:, "label"] = self.df[self.df._really_passed_now == True]\
            .TripState_expected_delay
        # Evolution of delay between last observed station and predicted
        # station
        self.df.loc[:, "label_ev"] = self.df[self.df._really_passed_now == True]\
            .apply(lambda x: x.label - x.last_observed_delay, axis=1)

    def _compute_pred_scores(self):
        """NAIVE PREDICTION:
        Naive prediction assumes that delay does not evolve:
        - evolution of delay = 0
        - delay predicted = last_observed_delay
        => error = real_delay - naive_pred
                 = label - last_observed_delay
                 = label_ev

        Scores for navie prediction for delay can be:
        - naive_pred_mae: mean absolute error: |label_ev|
        - naive_pred_mse: mean square error: (label_ev)**2

        API PREDICTION:
        We will be also able to compute scores for API predictions (when)
        retroactive is False.
        """
        self.df.loc[:, "naive_pred_mae"] = self.df["label_ev"].abs()
        self.df.loc[:, "naive_pred_mse"] = self.df["label_ev"]**2

    def get_rolling_trips(self, status=True):
        r = self\
            .trips_status[(self.trips_status > 0) & (self.trips_status < 1)]
        if status:
            return r
        else:
            return r.index

    def stats(self):
        assert self._realtime_computed

        message = """
        SUMMARY FOR DAY %(day)s AT TIME %(time)s (RETROACTIVE: %(retroactive)s)

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
            "retroactive": self.retro_active,
            "trips_today": len(self.df.Trip_trip_id.unique()),
            "trips_now": self.df
            .query("(trip_status > 0) & (trip_status < 1)")
            .Trip_trip_id.unique().shape[0],
            "trips_now_observed": self.df
            .query("(trip_status > 0) & (trip_status < 1) & (sequence_diff.notnull())")
            .Trip_trip_id.unique().shape[0],
            "stoptimes_today": self.df.Trip_trip_id.count(),
            "stoptimes_passed": self.df.TripState_passed_schedule.sum(),
            "stoptimes_passed_observed": self
            .df.TripState_passed_realtime.sum(),
            "stoptimes_not_passed": (~self.df.TripState_passed_schedule).sum(),
            "stoptimes_not_passed_observed": (self
                                              .df.TripState_passed_realtime == False).sum(),
            "stoptimes_now": self.df
            .query("(trip_status > 0) & (trip_status < 1)")
            .Trip_trip_id.count(),
            "stoptimes_now_passed": self.df
            .query("(trip_status > 0) & (trip_status < 1) &(TripState_passed_schedule==True)")
            .Trip_trip_id.count(),
            "stoptimes_now_passed_observed": self.df
            .query("(trip_status > 0) & (trip_status < 1) &(TripState_passed_realtime==True)")
            .Trip_trip_id.count(),
            "stoptimes_now_not_passed": self.df
            .query("(trip_status > 0) & (trip_status < 1) &(TripState_passed_schedule==False)")
            .Trip_trip_id.count(),
            "stoptimes_now_not_passed_observed": self.df
            .query("(trip_status > 0) & (trip_status < 1) &(TripState_passed_realtime==False)")
            .Trip_trip_id.count(),
            "stoptimes_predictable": self.df
            .query("(trip_status > 0) & (trip_status < 1) &(TripState_passed_schedule==False) & (sequence_diff.notnull())")
            .Trip_trip_id.count(),
            "stoptimes_predictable_labeled": self.df
            .query("(trip_status > 0) & (trip_status < 1) &(TripState_passed_schedule==False) & (sequence_diff.notnull()) &(label.notnull())")
            .Trip_trip_id.count(),
        }
        print(message % self.summary)

    def get_predictable(self, all_features_required=True, labeled_only=True, col_filter_level=2, split_datasets=False, set_index=True, provided_df=None):
        """Return predictable stop times.
        """
        assert self._realtime_computed

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
            "trip_status < 1 & trip_status > 0 & TripState_passed_schedule !=\
            True & TripState_passed_realtime != True")

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

        logger.info("Predictable with labeled_only=%s, has a total of %s rows." % (labeled_only, len(rdf))
                    )
        return rdf

    def _df_filter_cols(self, rdf, col_filter_level):
        # We need at least: index, features, and label
        filtered_cols = self._feature_cols\
            + self._id_cols\
            + self._label_cols\
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
            "y_naive_pred": rdf["last_observed_delay"]
        }
        return res

    def compute_multiple_times_of_day(self, begin="05:00:00", end="23:45:00", min_diff=60, flush_former=True, **kwargs):
        """Compute dataframes for different times of day.
        Note: for now, from 5AM (if no trip running, might cause problems)
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
            self.compute_for_time(step)
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

    def missing_data_per(self, per="Stop_stop_name"):
        # per can be also "Stop_stop_id", "Route_route_short_name"
        md = self.df.copy(deep=True)
        md["rt"] = ~md["RealTime_day_train_num"].isnull()
        md["rt"] = md.rt.apply(int)
        agg = md.groupby(per)["rt"].mean()
        return agg


def save_day_matrices_as_csv(start, end, data_path=None):
    """ Sart and end included
    """
    dti = pd.date_range(start=start, end=end, freq="D")
    days = dti.map(lambda x: x.strftime("%Y%m%d")).tolist()

    for day in days:
        mat = DayMatrixBuilder(day)
        mat.df.to_csv("%s%s" % (day, ".csv"))


# TODO
# A - take into account trip direction when computing delays on line
# B - handle cases where no realtime information is found (create nan cols)
# C - when retroactive is False, give api prediction as feature
# D - ability to save results in mongodb
# E - investigate missing values/stations
# F - perform trip_id train_num comparison on RATP lines
# G - improve speed by having an option to make computations only on running
# trains
