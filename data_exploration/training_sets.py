""" Module made to analyze training sets and provide predictions.
"""
from os import path
from glob import glob

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


###### DATA IMPORT AND SELECTION #######

# DATA IMPORT (after it has been loaded in temp file)
data_folder_path = "data/training_set-tempo-30-min/"
pickles = glob(path.join(data_folder_path, "*"))
# Day selection
start_date = "20170301"
end_date = "20170401"
dti = pd.date_range(start_date, end_date, freq="D")
days = dti.map(lambda x: x.strftime("%Y%m%d")).tolist()
pickles = [p for p in pickles if path.splitext(path.basename(p))[0] in days]

dfs = list(map(pd.read_pickle, pickles))
dfm = pd.concat(dfs)
dfm["dt"] = pd.to_datetime(dfm.index.get_level_values(0))

# SUBSAMPLE SELECTION
sel = dfm.copy()
# By line: selecting only some lines
lines = ['C', 'D', 'E', 'H', 'J', 'K', 'L', 'N', 'P', 'R', 'U']
# lines = ["C"]
mask = sel.index.get_level_values("Route_route_short_name_ix").isin(lines)
sel = sel[mask]
# By sequence: selection only prediction for 1 to 10 stations ahead
min_diff = 1
max_diff = 40
cond1 = (sel.index.get_level_values("sequence_diff_ix") >= min_diff)
cond2 = (sel.index.get_level_values("sequence_diff_ix") <= max_diff)
mask = cond1 & cond2
sel = sel[mask]


###### NAIVE PREDICTION SCORES ANALYSIS #######

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

###### SCORE COMPARISON ANALYSIS: #######
# CUSTOM PREDICTION VS NAIVE PREDICTION
