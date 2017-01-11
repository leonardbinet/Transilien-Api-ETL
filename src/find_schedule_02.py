import pandas as pd
import os

data_path = "../data/"
trips = pd.read_csv(os.path.join(data_path, "trips.txt"))
calendar = pd.read_csv(os.path.join(data_path, "calendar.txt"))
stop_times = pd.read_csv(os.path.join(data_path, "stop_times.txt"))

df_merged = stop_times.merge(trips, on="trip_id", how="left")
df_merged = df_merged.merge(calendar, on="service_id", how="left")

df_merged["train_id"] = df_merged.trip_id.str.extract("^.{5}(\d{6})")
