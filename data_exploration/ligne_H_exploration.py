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

stops_matrix = sel.pivot(
    index="Stop_stop_id",
    columns="Trip_trip_id",
    values="D_stop_scheduled_datetime"
)

stops_matrix = stations\
    .join(stops_matrix, on="stop_id", how="right")\
    .set_index("stop_id")

# Selection of stations on Trip with long sequence
trip_id = "DUASN124663F01001-1_419131"
trip_sequence = stops_matrix.loc[:, trip_id]
trip_sequence = trip_sequence[trip_sequence.notnull()].sort_values()

stops_matrix_on_section = stops_matrix.loc[trip_sequence.index, :]

min_numbers_on_section = 5
stops_matrix_on_section = stops_matrix_on_section\
    .loc[:, stops_matrix_on_section.notnull().sum(axis=0) >=
         min_numbers_on_section]
