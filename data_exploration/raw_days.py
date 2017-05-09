"""Data exploration of raw passages
"""

from os import path
from glob import glob

import pandas as pd
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import seaborn as sns

from api_etl.utils_misc import DateConverter

############# DATA IMPORT AND INITIAL CLEANUP #############

data_folder_path = "data/raw_days/"
pickles = glob(path.join(data_folder_path, "*"))
# Day selection (don't take to much or it might crash your memory)
start_date = "20170215"
end_date = "20170216"
dti = pd.date_range(start_date, end_date, freq="D")
days = dti.map(lambda x: x.strftime("%Y%m%d")).tolist()
# names are raw_yyyymmdd.pickle
dfs = []
for p in pickles:
    day = path.splitext(path.basename(p))[0][4:]
    if day in days:
        dfd = pd.read_pickle(p)
        dfd.loc[:, "stoptime_day"] = day
        dfs.append(dfd)


dfm = pd.concat(dfs)
dfm["stop_scheduled_datetime"] = dfm.apply(
    lambda x: DateConverter(
        special_time=x.StopTime_departure_time,
        special_date=x.stoptime_day).dt,
    axis=1
)
dfm.set_index(
    ["stoptime_day", "Trip_trip_id", "Stop_stop_id", "stop_scheduled_datetime"],
    inplace=True
)

############# SCHEDULE EXPLORATION #############


# Find out how many trains are traveling on a given line, at a given time

# Find out how many stops per hour in a given station
# Problem: does not take into account all trains not stopping
all_stations_stops = dfm\
    .groupby(["Stop_stop_id", dfm.index.dayofyear, dfm.index.hour])[
        "StopTime_departure_time"]\
    .count().unstack().T
all_stations_stops.plot(legend=False)
plt.title("Number of train stops scheduled per hour in given station")
plt.show()

# Show only stations with most important peaks
peak_quantile = all_stations_stops.max(axis=0).quantile(0.8)
all_stations_stops.loc[:, all_stations_stops.max(
    axis=0) > peak_quantile].plot(legend=False)
plt.title("Stations with most important peaks (0.8 quantile)")
plt.show()

# Map of stations with peaks values
# in another file


############# REALTIME DATA EXPLORATION #############

# Df2 contains only stops where trip_id is identified
df2 = dfm.dropna(subset=["Trip_trip_id"], how="any", axis=0)

# Identified number of stations per trip
df2.groupby(["Trip_trip_id", "RealTime_expected_passage_day"])[
    "Stop_stop_id"].count().hist()
plt.title("Identified number of stations stops per trip")
plt.show()

# Get sequences per day_trip_id
df2["day_trip_id"] = df2.apply(lambda x: "%s_%s" % (
    x["expected_passage_day"], x["trip_id"]), axis=1)
sequences = df2.pivot(index="stop_sequence",
                      columns="day_trip_id", values="delay")

# Select only sequences long enough (> 15)
long_seqs = sequences.loc[:, ~sequences.loc[15.0, :].isnull()]
# or long_seqs = sequences.T[~sequences.T[15.0].isnull()].T
# select only 200 to plot, and to max 19 stops
long_seqs.iloc[:19, :200].plot(legend=False)
plt.title("Sequences sample: delays over itinerary")
plt.show()

# Very delayed mean: 0.9 quantile
quant_value = long_seqs.mean().quantile(0.9)
delayed_long_seq = long_seqs.loc[:, long_seqs.mean() > quant_value]
delayed_long_seq.iloc[:19, :50].plot(legend=False)
plt.title("Sequences 0.9 quantile for mean delay sample: delays over itinerary")
plt.show()

# Very delayed max: 0.9 quantile
quant_value = long_seqs.max().quantile(0.9)
delayed_long_seq = long_seqs.loc[:, long_seqs.max() > quant_value]
delayed_long_seq.iloc[:19, :50].plot(legend=False)
plt.title("Sequences 0.9 quantile for max delay sample: delays over itinerary")
plt.show()

# Time sequences (instead of station sequence number, we use time)
# time_sequences = df2.pivot(columns="day_trip_id", values="delay")


# Delays per hour and per line
df2.groupby([df2.index.hour, "route_short_name"])[
    "delay"].mean().unstack().plot()
plt.title("Mean delays per hour and per line")
plt.show()

# Delays per hour and per weekday
df2.groupby([df2.index.hour, df2.index.weekday])[
    "delay"].mean().unstack().plot()
plt.title("Mean delays per hour and per weekday")
plt.show()
