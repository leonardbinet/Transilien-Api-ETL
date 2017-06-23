"""
This module contains functions and classes to train regressors and save them into SQL database.
"""

import logging
from os import path
from glob import glob
import pickle
from datetime import datetime
import codecs

import numpy as np
import pandas as pd

import sklearn
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from sklearn.metrics import explained_variance_score, r2_score, mean_squared_error, mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline

from api_etl.settings import __DATA_PATH__, __TRAINING_FEATURE_NAMES__, __ACCEPTED_LINES__
# from api_etl.utils_misc import S3Bucket
from api_etl.utils_rdb import rdb_provider
from api_etl.data_models import Predictor

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import seaborn as sns


class RegressorTrainer:
    """
    This class allows you to easily load datasets and train regressors for a given line, and save it in a database.
    """

    def __init__(self, line="C", auto=False):
        assert line in __ACCEPTED_LINES__
        self.line = line
        self.scaler = None
        self.regressor = None
        self.pipeline = None
        self.features = __TRAINING_FEATURE_NAMES__
        self.start_date = None
        self.end_date = None

        self.dfm = None  # whole training set before filtering
        self.sel = None  # selection

        self._fitted = False
        self._big_label_delay_mask = None
        self._big_last_delay_mask = None
        self.delay_threshold = None

        if auto:
            self.set_feature_cols()
            self.build_training_set(from_folder=True)
            self.build_pipeline()
            self.train_pipeline()

    def set_feature_cols(self, *args):
        args = args or __TRAINING_FEATURE_NAMES__
        self.features = args

    def build_training_set(self, start_date="20170215", end_date="20170401",
                           tempo=30, from_folder=None, **kwargs):
        """
        Either from folder, either from S3
        :param start_date:
        :param end_date:
        :param tempo:
        :return:
        """

        # ARGS PARSING
        assert isinstance(tempo, int)
        datetime.strptime(start_date, "%Y%m%d")
        self.start_date = start_date
        datetime.strptime(end_date, "%Y%m%d")
        self.end_date = end_date

        # Training sets are stored by day
        dti = pd.date_range(start_date, end_date, freq="D")
        days = dti.map(lambda x: x.strftime("%Y%m%d")).tolist()

        if from_folder:
            dfm = self._load_files_from_folder(tempo=tempo, days=days)

        else:
            dfm = self._load_files_from_s3()

        # delete duplicated columns
        dfm = dfm.loc[:, ~dfm.columns.duplicated()]
        self._raw_training_set = dfm

        # Line selection
        self.sel = dfm.copy()
        # Apply line selection
        self._filter_line(line=self.line)
        # Apply other filter
        self._apply_filter_to_selection(**kwargs)

        # Train and test
        self._split_train_test()

    def _load_files_from_s3(self):
        # TODO
        df = pd.DataFrame()
        return df

    def _load_files_from_folder(self, tempo, days):
        # Load training sets either from S3 or from local
        __TRAINING_SETS_FOLDER_PATH__ = path.join(__DATA_PATH__, "training_set-tempo-%s-min" % tempo)
        training_set_pickles = glob(path.join(__TRAINING_SETS_FOLDER_PATH__, "*"))

        training_set_pickles = [p for p in training_set_pickles if path.splitext(path.basename(p))[0] in days]
        # Load and concatenate training sets
        dfs = list(map(pd.read_pickle, training_set_pickles))
        dfm = pd.concat(dfs)
        return dfm

    def _filter_line(self, line):
        assert line in __ACCEPTED_LINES__
        self.line = line
        mask = (self.sel.index.get_level_values("Route_route_short_name_ix") == line)
        self.sel = self.sel[mask]

    def _apply_filter_to_selection(self, min_diff=1, max_diff=40, min_delay=-3000, max_delay=10000):
        sel = self.sel

        # By sequence: selection only prediction for 1 to 10 stations ahead
        cond1 = (sel.index.get_level_values("TS_sequence_diff_ix") >= min_diff)
        cond2 = (sel.index.get_level_values("TS_sequence_diff_ix") <= max_diff)
        mask = cond1 & cond2
        sel = sel[mask]

        # Per delay
        cond1 = (self.sel["label"] >= min_delay)
        cond2 = (self.sel["label"] <= max_delay)
        mask = cond1 & cond2
        sel = sel[mask]

        self.sel = sel

    def _split_train_test(self):

        self.X = self.sel[self.features]
        self.y_naive_pred = self.sel["P_naive_pred"]
        self.y = self.sel["label"]

        self.X_train, self.X_test, self.y_train, self.y_test, self.y_naive_pred_train, self.y_naive_pred_test = \
            train_test_split(
                self.X, self.y, self.y_naive_pred,
                test_size=0.30, random_state=1
            )

    def build_pipeline(self, scale=True, polynomial=False, regressor=LinearRegression):

        steps = []
        # Scaling preprocessing
        if scale:
            scaler = StandardScaler(copy=True).fit(self.X)
            steps.append(("scaler", scaler))

        # Polynomial preprocessing
        if polynomial:
            poly = PolynomialFeatures(2)
            steps.append(("polynomial", poly))

        steps.append(("regressor", regressor()))

        self.pipeline = Pipeline(steps=steps)

    def train_pipeline(self, delay_threshold=None, last_delay_threshold=None):
        # accepts only one of two
        assert isinstance(delay_threshold, int) or delay_threshold is None
        assert isinstance(last_delay_threshold, int) or last_delay_threshold is None
        assert not ((last_delay_threshold is not None) and (delay_threshold is not None))

        X_train = self.X_train
        y_train = self.y_train

        if delay_threshold is not None:
            # Per delay
            self._big_label_delay_mask = (self.sel["label"] >= delay_threshold)
            X_train = X_train[self._big_label_delay_mask]
            y_train = y_train[self._big_label_delay_mask]
            self.delay_threshold = delay_threshold

        if last_delay_threshold is not None:
            self._big_last_delay_mask = self.sel.loc[:,"TS_last_observed_delay"] >= last_delay_threshold
            X_train = X_train[self._big_last_delay_mask]
            y_train = y_train[self._big_last_delay_mask]

        self.pipeline.fit(X_train, y_train)
        self._fitted = True

    def score_pipeline(self):
        assert self._fitted

        y_pred = self.pipeline.predict(self.X_test)
        # SCORES COMPARISON
        # Score comparison between naive prediction, and custom predictions
        message = "\n\nPREDICTIONS ALL:"
        message += self.show_scores(name="Regressor", y_true=self.y_test, y_pred=y_pred)
        message += self.show_scores(name="Naive", y_true=self.y_test, y_pred=self.y_naive_pred_test)

        if self._big_label_delay_mask is not None:
            y_pred_bd = self.pipeline.predict(self.X_test[self._big_label_delay_mask])
            y_naive_pred_test_bd = self.y_naive_pred_test[self._big_label_delay_mask]
            y_pred_sd = self.pipeline.predict(self.X_test[~self._big_label_delay_mask])
            y_naive_pred_test_sd = self.y_naive_pred_test[~self._big_label_delay_mask]

            message += "\n\nPREDICTIONS BIG DELAYS:"
            message += self.show_scores(name="Regressor", y_true=self.y_test[self._big_label_delay_mask], y_pred=y_pred_bd)
            message += self.show_scores(name="Naive", y_true=self.y_test[self._big_label_delay_mask], y_pred=y_naive_pred_test_bd)

            message += "\n\nPREDICTIONS SMALL DELAYS:"
            message += self.show_scores(name="Regressor", y_true=self.y_test[~self._big_label_delay_mask], y_pred=y_pred_sd)
            message += self.show_scores(name="Naive", y_true=self.y_test[~self._big_label_delay_mask], y_pred=y_naive_pred_test_sd)

        if self._big_last_delay_mask is not None:
            y_pred_bd = self.pipeline.predict(self.X_test[self._big_last_delay_mask])
            y_naive_pred_test_bd = self.y_naive_pred_test[self._big_last_delay_mask]
            y_pred_sd = self.pipeline.predict(self.X_test[~self._big_last_delay_mask])
            y_naive_pred_test_sd = self.y_naive_pred_test[~self._big_last_delay_mask]

            message += "\n\nPREDICTIONS BIG DELAYS:"
            message += self.show_scores(name="Regressor", y_true=self.y_test[self._big_last_delay_mask], y_pred=y_pred_bd)
            message += self.show_scores(name="Naive", y_true=self.y_test[self._big_last_delay_mask], y_pred=y_naive_pred_test_bd)

            message += "\n\nPREDICTIONS SMALL DELAYS:"
            message += self.show_scores(name="Regressor", y_true=self.y_test[~self._big_last_delay_mask], y_pred=y_pred_sd)
            message += self.show_scores(name="Naive", y_true=self.y_test[~self._big_last_delay_mask], y_pred=y_naive_pred_test_sd)


        return message

    def show_scores(self, name, y_true, y_pred, all=False):
        message = "\n%s SCORES" % name.upper()
        if all:
            message += "\n%s R2 score: %s" % (name, r2_score(y_true=y_true, y_pred=y_pred))
            message += "\n%s Explained variance: %s" % (name, explained_variance_score(y_true, y_pred))
            message += "\n%s Mean square error: %s" % (name, mean_squared_error(y_true, y_pred))

        message += "\n%s Mean absolute error: %s" % (name, mean_absolute_error(y_true, y_pred))
        return message

    def save_in_database(self):
        assert self._fitted

        pickled_pipeline = codecs.encode(pickle.dumps(self.pipeline), "base64").decode()

        # init predictor
        predictor = Predictor(
            pipeline=pickle.dumps(self.pipeline),
            features=self.features,
            line=self.line,
            delay_threshold=self.delay_threshold,
            training_set_start=self.start_date,
            training_set_end=self.end_date,
            sklearn_version=sklearn.__version__,
            pipeline_steps=self.pipeline.steps.__str__(),
            score_description=self.score_pipeline()
        )

        # save in db
        session = rdb_provider.get_session()
        try:
            session.add(predictor)
            session.commit()
        except:
            session.rollback()
        session.close()

    def analyze_scores(self):
        y_pred = self.pipeline.predict(self.X_test)

        comparison_df = pd.DataFrame(
            data={"r": self.y_test.values, "p": y_pred},
            index=self.y_test.index)

        comparison_df["reg_abs_error"] = np.abs(comparison_df.r - comparison_df.p)
        comparison_df["reg_sqr_error"] = comparison_df["reg_abs_error"]**2
        comparison_df = pd.concat([self.X_test, comparison_df], axis=1)

        comparison_df["naive_abs_error"] = np.abs(comparison_df.r - comparison_df.TS_last_observed_delay)
        comparison_df["naive_sqr_error"] = comparison_df["naive_abs_error"]**2

        # PLOTS
        # per sequence diff
        comparison_df.groupby(level=5)[["reg_abs_error","naive_abs_error"]].mean().plot()
        plt.title("Regressor prediction mean square error, per sequence_diff")
        plt.show()

        # per label
        comparison_df.groupby("r")[["reg_abs_error","naive_abs_error"]].mean().plot()
        plt.title("Regressor prediction mean square error, per label delay")
        plt.show()

        # per last_observed_delay
        comparison_df.groupby("TS_last_observed_delay")[["reg_abs_error","naive_abs_error"]].mean().plot()
        plt.title("Regressor prediction mean square error, per label delay")
        plt.show()

        # per number_of_trains
        comparison_df.groupby("TS_rolling_trips_on_line")[["reg_abs_error","naive_abs_error"]].mean().plot()
        plt.title("Regressor prediction mean square error, per label delay")
        plt.show()

        comparison_df.groupby(level=[2, 5]).reg_abs_error.mean().unstack().T.plot()
        plt.title("Regressor prediction mean square error, per sequence_diff and mission code")
        plt.show()


