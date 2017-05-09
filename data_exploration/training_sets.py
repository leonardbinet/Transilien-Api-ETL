""" Module made to analyze training sets and provide predictions.

Parameters to chose:
- lines considered
- sequence_diff considered (predictions for how many stations ahead)

Then you should compute your own predictions on the test sample and assign it
to the y_pred variable so that plot and scores are computed.
"""
from os import path
from glob import glob

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

###### DATA IMPORT AND SELECTION #######

# DATA IMPORT (after it has been loaded in temp file)
data_folder_path = "data/training_set-tempo-30-min/"
pickles = glob(path.join(data_folder_path, "*"))
# Day selection
start_date = "20170215"
end_date = "20170401"
dti = pd.date_range(start_date, end_date, freq="D")
days = dti.map(lambda x: x.strftime("%Y%m%d")).tolist()
pickles = [p for p in pickles if path.splitext(path.basename(p))[0] in days]

dfs = list(map(pd.read_pickle, pickles))
dfm = pd.concat(dfs)
# problem saving duplicated columns
dfm = dfm.loc[:, ~dfm.columns.duplicated()]
# dfm["dt"] = pd.to_datetime(dfm.index.get_level_values(0))

# SUBSAMPLE SELECTION
sel = dfm.copy()
# By line: selecting only some lines
# lines = ['C', 'D', 'E', 'H', 'J', 'K', 'L', 'N', 'P', 'R', 'U']
lines = ["C", "E"]
mask = sel.index.get_level_values("Route_route_short_name_ix").isin(lines)
sel = sel[mask]
# By sequence: selection only prediction for 1 to 10 stations ahead
min_diff = 1
max_diff = 40
cond1 = (sel.index.get_level_values("sequence_diff_ix") >= min_diff)
cond2 = (sel.index.get_level_values("sequence_diff_ix") <= max_diff)
mask = cond1 & cond2
sel = sel[mask]
# By scheduled trip time:
scheduled_trip_filter = False
if scheduled_trip_filter:
    min_trip = 300
    max_trip = 4000
    cond1 = (sel["stations_scheduled_trip_time"] >= min_trip)
    cond2 = (sel["stations_scheduled_trip_time"] <= max_trip)
    mask = cond1 & cond2
    sel = sel[mask]
    # Only those that have a large delay
    min_delay = 180
    max_delay = 3000
# Per delay
delay_filter = False
if delay_filter:
    min_delay = -3000
    max_delay = 3000
    cond1 = (sel["label"] >= min_delay)
    cond2 = (sel["label"] >= max_delay)
    mask = cond1 & cond2
    sel = sel[mask]

###### NAIVE PREDICTION SCORES ANALYSIS #######
print_naive_scores = False

if print_naive_scores:
    groupbies = [
        "Stop_stop_id_ix", "sequence_diff_ix", "RealTime_miss_ix",
        "Route_route_short_name_ix"
    ]

    for groupby in groupbies:
        sel.groupby(level=groupby)["naive_pred_mae"].mean().plot()
        plt.show()
        sel.groupby(level=groupby)["naive_pred_mse"].mean().plot()
        plt.show()


###### CUSTOM PREDICTION COMPUTATION #######

from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import explained_variance_score, r2_score, mean_squared_error, mean_absolute_error
# from sklearn.model_selection import RandomizedSearchCV

_feature_cols = [
    # "Route_route_short_name", # condidering only C
    "last_observed_delay",
    "line_station_median_delay",
    "line_median_delay",
    "Trip_direction_id",
    "sequence_diff",
    "stations_scheduled_trip_time",
    "rolling_trips_on_line",
    #    "stoptime_scheduled_hour",
    #    "RealTime_miss",
    "business_day"
]


def show_scores(name, y_true, y_pred):
    print("%s R2 score: %s" % (name, r2_score(y_true=y_true, y_pred=y_pred)))
    print("%s Explained variance: %s" % (name,
                                         explained_variance_score(y_true, y_pred)))
    print("%s Mean square error: %s" % (name,
                                        mean_squared_error(y_true, y_pred)))
    print("%s Mean absolute error: %s" % (name,
                                          mean_absolute_error(y_true, y_pred)))


X = sel[_feature_cols]
y_naive_pred = sel.naive_pred
y = sel.label

# TEST SAMPLE

X_train, X_test, y_train, y_test, y_naive_pred_train, y_naive_pred_test = train_test_split(
    X, y, y_naive_pred,
    test_size=0.30, random_state=1
)

# To keep initial state before preprocessing operations
X_train_ini = X_train.copy()
X_test_ini = X_test.copy()

# SCALING
scale = True
if scale:
    scaler = preprocessing.StandardScaler().fit(X_train)
    X_train = pd.DataFrame(
        data=scaler.transform(X_train),
        index=X_train.index,
        columns=X_train.columns.values
    )
    X_test = pd.DataFrame(
        data=scaler.transform(X_test),
        index=X_test.index,
        columns=X_test.columns.values
    )

# Polynomial preprocessing: many features are correlated
from sklearn.preprocessing import PolynomialFeatures

polynomial = False
if polynomial:
    poly = PolynomialFeatures(2)
    X_train = pd.DataFrame(
        data=poly.fit_transform(X_train),
        index=X_train.index,
        columns=poly.get_feature_names(X_train.columns)
    )
    X_test = pd.DataFrame(
        data=poly.fit_transform(X_test),
        index=X_test.index,
        columns=poly.get_feature_names(X_test.columns)
    )


regr = LinearRegression()
regr.fit(X_train, y_train)

y_pred = regr.predict(X_test)

# Here, your predictions on the test subsample should be assigned
# to y_pred variable

# SCORES COMPARISON
# Score comparison between
show_scores(name="Custom Pred", y_true=y_test, y_pred=y_pred)
show_scores(name="Naive Pred", y_true=y_test, y_pred=y_naive_pred_test)

comparison_df = pd.DataFrame(
    data={"r": y_test.values, "p": y_pred}, index=y_test.index)
comparison_df["abs_error"] = np.abs(comparison_df.r - comparison_df.p)
comparison_df["sqr_error"] = comparison_df["abs_error"]**2
comparison_df = pd.concat([X_test_ini, comparison_df], axis=1)

# PLOTS
comparison_df.groupby(level=5).abs_error.mean().plot()
plt.title("Prediction mean square error, per sequence_diff")
plt.show()

comparison_df.groupby(level=[2, 5]).abs_error.mean().unstack().T.plot()
plt.title("Prediction mean square error, per sequence_diff and mission code")
plt.show()

if len(lines) > 1:
    comparison_df.groupby(level=1).abs_error.mean().plot(kind="bar")
    plt.title("Prediction mean square error, per line")
    plt.show()

    comparison_df.groupby(level=[1, 5]).abs_error.mean().unstack().T.plot()
    plt.title("Prediction mean square error, per line and sequence_diff")
    plt.show()
