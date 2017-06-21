"""
This module contains functions and classes to train regressors and make predictions.
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

from api_etl.settings import __DATA_PATH__, __FEATURE_MAPPING__, __TRAINING_FEATURE_NAMES__
from api_etl.utils_misc import S3Bucket
from api_etl.utils_rdb import rdb_provider
from api_etl.models import Predictor
from api_etl.feature_vector import StopTimeFeatureVector

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None



# Label columns
_label_cols = ["label", "label_ev"]

# Scoring columns
_scoring_cols = ["S_naive_pred_mae", "S_naive_pred_mse"]

# Prediction columns
_prediction_cols = ["P_api_pred", "P_api_pred_ev", "P_naive_pred"]

__ACCEPTED_LINES__ = ['C', 'D', 'E', 'H', 'J', 'K', 'L', 'N', 'P', 'R', 'U']


class RegressorTrainer:
    """
    This class allows you to easily load datasets and train regressors for a given line, and save it in a database.
    """

    def __init__(self, auto=False):
        self.line = None
        self.scaler = None
        self.regressor = None
        self.pipeline = None
        self.features = __TRAINING_FEATURE_NAMES__
        self.start_date = None
        self.end_date = None

        self.dfm = None  # whole training set before filtering
        self.sel = None  # selection

        self._fitted = False
        self._big_delay_mask = None
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
                           tempo=30, from_folder=None, line="C", **kwargs):
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
        self._filter_line(line=line)
        # Apply other filter
        self._apply_filter_to_selection(**kwargs)

        # Train and test
        self._split_train_test()

    def _load_files_from_s3(self):
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

    def train_pipeline(self, delay_threshold=600):
        assert isinstance(delay_threshold, int) or delay_threshold is None
        X_train = self.X_train
        y_train = self.y_train

        if delay_threshold is not None:
            # Per delay
            self._big_delay_mask = (self.sel["label"] >= delay_threshold)
            X_train = X_train[self._big_delay_mask]
            y_train = y_train[self._big_delay_mask]
            self.delay_threshold = delay_threshold

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

        if self._big_delay_mask is not None:
            y_pred_bd = self.pipeline.predict(self.X_test[self._big_delay_mask])
            y_naive_pred_test_bd = self.y_naive_pred_test[self._big_delay_mask]
            y_pred_sd = self.pipeline.predict(self.X_test[~self._big_delay_mask])
            y_naive_pred_test_sd = self.y_naive_pred_test[~self._big_delay_mask]

            message += "\n\nPREDICTIONS BIG DELAYS:"
            message += self.show_scores(name="Regressor", y_true=self.y_test[self._big_delay_mask], y_pred=y_pred_bd)
            message += self.show_scores(name="Naive", y_true=self.y_test[self._big_delay_mask], y_pred=y_naive_pred_test_bd)

            message += "\n\nPREDICTIONS SMALL DELAYS:"
            message += self.show_scores(name="Regressor", y_true=self.y_test[~self._big_delay_mask], y_pred=y_pred_sd)
            message += self.show_scores(name="Naive", y_true=self.y_test[~self._big_delay_mask], y_pred=y_naive_pred_test_sd)

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


class RegressorPredictor:
    """
    Class used to apply regressor pipeline on feature vectors.
    """

    def __init__(self, line="C", filter_sklearn_version=False):
        self.predictor = None
        self.vector_requested_features = None
        self.unpickled_pipeline = None

        assert line in __ACCEPTED_LINES__
        self.line = line

        # Gets the last created on this line
        session = rdb_provider.get_session()
        query = session.query(Predictor)
        if filter_sklearn_version:
            query = query.filter(Predictor.sklearn_version==sklearn.__version__)

        predictor = query\
            .filter(Predictor.line == line)\
            .order_by(Predictor.created_date.desc())\
            .first()
        session.close()

        assert isinstance(predictor, Predictor)
        logger.info("Queried last predictor for line %s and got %s" % (line, predictor))
        self.predictor = predictor
        # check that you are using same version of sklearn
        if predictor.sklearn_version != sklearn.__version__:
            logger.warn("Sklearn versions are not the same, the use of pipeline pickle might not work! %s vs %s" %
                        (predictor.sklearn_version, sklearn.__version__))

        # unpickle pipeline
        self.unpickled_pipeline = pickle.loads(self.predictor.pipeline)

        # translate requested features
        self.vector_requested_features = []
        for matrix_feature in self.predictor.features:
            self.vector_requested_features.append(__FEATURE_MAPPING__[matrix_feature])

    def predict(self, feature_vectors):
        """
        Returns results in same order.

        :param feature_vectors:
        :return:
        """
        # first, a predictor must have been downloaded
        assert self.predictor is not None

        # accept a single element or list
        if not isinstance(feature_vectors, list):
            feature_vectors = [feature_vectors]

        # check that all elements passed are real feature vectors
        for el in feature_vectors:
            assert isinstance(el, StopTimeFeatureVector)

        return list(map(self.predict_one, feature_vectors))

    def predict_one(self, feature_vector):
        assert self.predictor is not None
        assert isinstance(feature_vector, StopTimeFeatureVector)
        # if not all requested features are present, return None
        if not feature_vector.has_features(*self.vector_requested_features):
            logger.info("Feature vector %s has not all requested features (%s)." %
                        (feature_vector, self.vector_requested_features))
            return None

        requested_vector = [getattr(feature_vector, feature) for feature in self.vector_requested_features]
        requested_vector = np.array(requested_vector).reshape(1, -1)
        prediction = self.unpickled_pipeline.predict(requested_vector)
        logger.info("Predicted delay of %s seconds"% prediction[0])
        return prediction[0]
