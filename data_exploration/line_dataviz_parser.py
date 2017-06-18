"""
Preprocessing steps to get data in right format so that it can be displayed.

Data should end in the following format: list of:

begin                                           1494448500
end                                             1494458543
line                                                  "H"
stops  [{'stop_id': 'StopPoint:DUA8727613', 'time': 1391422380}, ...
trip                            "DUASN847548F01001-1_408444"

"""

import pandas as pd
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import seaborn as sns

day_data_path = "20170510.pickle"
df = pd.read_pickle(day_data_path)
stations = pd.read_csv("data/gtfs-lines-last/stops.txt")

# Subset selection: columns, and rows
cols_to_keep = [
    "StopTime_stop_sequence", "Trip_trip_id",
    "Stop_stop_id", "D_stop_scheduled_datetime"
]
line = "H"
sel = df.query("Route_route_short_name==@line")\
    .loc[:, cols_to_keep]

sel["D_stop_scheduled_datetime"] = sel.D_stop_scheduled_datetime\
    .apply(lambda x: x.timestamp())

stops_matrix = sel.pivot(
    index="Trip_trip_id",
    columns="Stop_stop_id",
    values="D_stop_scheduled_datetime"
)

# stops_matrix = stations\
#    .join(stops_matrix, on="stop_id", how="right")\
#    .set_index("stop_id").T

""" FOR CUSTOM SECTION
# Selection of stations on Trip with long sequence
trip_id = "DUASN124663F01001-1_419131"
trip_sequence = stops_matrix.loc[:, trip_id]
trip_sequence = trip_sequence[trip_sequence.notnull()].sort_values()

stops_matrix_on_section = stops_matrix.loc[trip_sequence.index, :]

min_numbers_on_section = 5
stops_matrix_on_section = stops_matrix_on_section\
    .loc[:, stops_matrix_on_section.notnull().sum(axis=0) >=
         min_numbers_on_section]
"""

stops_matrix.loc[:, "stops"] = stops_matrix\
    .apply(lambda x: x.to_dict(), axis=1)\
    .apply(lambda x: [{"stop_id": k, "time": x[k]} for k in x if pd.notnull(x[k])])\
    .apply(lambda x: sorted(x, key=lambda y: y["time"]))

stops_matrix.loc[:, "begin"] = stops_matrix.min(axis=1)
stops_matrix.loc[:, "end"] = stops_matrix.max(axis=1)

stops_matrix.loc[:, "trip"] = stops_matrix.index
stops_matrix.loc[:, "line"] = "H"

final_cols = ["begin", "end", "stops", "trip", "line"]
stops_matrix.loc[:, final_cols].to_json("trips_stops.json", orient="records")
