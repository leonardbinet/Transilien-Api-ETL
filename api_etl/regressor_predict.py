"""
Class used to extract regressor from database and apply it.
"""

import logging
import pickle

import numpy as np
import sklearn

from api_etl.settings import __FEATURE_MAPPING__, __ACCEPTED_LINES__
from api_etl.utils_rdb import rdb_provider
from api_etl.data_models import Predictor
from api_etl.feature_vector import StopTimeFeatureVector

logger = logging.getLogger(__name__)


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
            query = query.filter(Predictor.sklearn_version == sklearn.__version__)

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
        logger.info("Predicted delay of %s seconds" % prediction[0])
        return prediction[0]
