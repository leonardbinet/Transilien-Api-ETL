"""
In separate module to avoid cyclic import.
"""

import logging
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


def has_features(obj, *features):
    for feature in features:
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
class_dict.update({
    "set_features": set_features,
    "is_complete": is_complete,
    "__repr__": __repr__,
    "__str__": __str__,
    "has_features": has_features,
})

StopTimeFeatureVector = type("StopTimeFeatureVector", (object,), class_dict)