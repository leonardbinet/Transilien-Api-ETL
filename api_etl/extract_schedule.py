"""
Module used to download from SNCF website trains schedules and save it in the right format in different databases (Dynamo or relational database)
"""

from os import path
import pandas as pd
import zipfile
import time
from urllib.request import urlretrieve
import logging
from datetime import datetime
import calendar


from api_etl.settings import (
    data_path, gtfs_csv_url, dynamo_sched_dep,
    shed_read, shed_write_on, shed_write_off
)
from api_etl.utils_dynamo import (
    dynamo_update_provisionned_capacity, ScheduledDeparture
)
from api_etl.utils_rdb import Provider
from api_etl.models import (
    Agency,
    Route,
    Trip,
    StopTime,
    Stop,
    Calendar,
    CalendarDate
)

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


class ScheduleExtractor():
    """ Common class for schedule extractors
    """

    def __init__(self, data_folder=None, schedule_url=None):
        # Place where gtfs folder is supposed to be located
        # If none specified, takes default folder specified in settings
        if not data_folder:
            data_folder = data_path
        self.data_folder = data_folder
        # Gtfs folder
        self.gtfs_folder = path.join(self.data_folder, "gtfs-lines-last")

        # URL from which we download gtfs files
        # If none specified, takes default url specified in settings
        if not schedule_url:
            schedule_url = gtfs_csv_url
        self.schedule_url = schedule_url

        self.files_present = None
        self._check_files()

    def _check_files(self):
        files_to_check = [
            "calendar.txt",
            "trips.txt",
            "stop_times.txt",
            "stops.txt",
            "calendar_dates.txt",
        ]
        try:
            for file_check in files_to_check:
                test_df = pd.read_csv(
                    path.join(self.gtfs_folder, file_check)
                )
            self.files_present = True
        except FileNotFoundError as e:
            logging.warning("File %s not found in data folder %s" %
                            (file_check, self.gtfs_folder))
            self.files_present = False

    def download_gtfs_files(self):
        """
        Download gtfs files from SNCF website (based on URL defined in settings module) and saves it in data folder (defined as well in settings module). There is no paramater to pass.

        Process is in two steps:
        - first: download csv file containing links to zip files
        - second: download files based on urls found in csv from first step

        Folder names in which files are unzip are based on the headers of the zip files.

        Function returns True if 'gtfs-lines-last' folder has been found (this is the usual folder we use then to find schedules). Return False otherwise.

        :rtype: boolean
        """
        logger.info(
            "Download of csv containing links of zip files, at url %s", self.schedule_url)
        df_links_gtfs = pd.read_csv(self.schedule_url)

        # Download and unzip all files
        # Check if one is "gtfs-lines-last" (necessary)
        gtfs_lines_last_present = False
        for link in df_links_gtfs["file"].values:
            logger.info("Download of %s", link)
            local_filename, headers = urlretrieve(link)

            logger.info("File name is %s", headers.get_filename())
            # Get name in header and remove the ".zip"
            extracted_data_folder_name = headers.get_filename().split(".")[0]
            if extracted_data_folder_name == "gtfs-lines-last":
                gtfs_lines_last_present = True

            with zipfile.ZipFile(local_filename, "r") as zip_ref:
                full_path = path.join(
                    self.data_folder, extracted_data_folder_name)
                zip_ref.extractall(path=full_path)

            if gtfs_lines_last_present:
                logger.info("The 'gtfs-lines-last' folder has been found.")
                return True
            else:
                logger.error(
                    "The 'gtfs-lines-last' folder has not been found! Schedules will not be updated.")
                return False


class ScheduleExtractorRDB(ScheduleExtractor):
    """ For relational database
    """

    def __init__(self, data_folder=None, schedule_url=None, dsn=None):
        ScheduleExtractor.__init__(
            self, data_folder=data_folder, schedule_url=schedule_url)

        self.dsn = dsn
        self.rdb_provider = Provider(self.dsn)

    def save_in_rdb(self, tables=None):
        assert self.files_present

        to_save = [
            ("agency.txt", Agency),
            ("routes.txt", Route),
            ("trips.txt", Trip),
            ("stops.txt", Stop),
            ("stop_times.txt", StopTime),
            ("calendar.txt", Calendar),
            ("calendar_dates.txt", CalendarDate)
        ]
        if tables:
            assert isinstance(tables, list)
            to_save = [to_save[i] for i in tables]

        for name, model in to_save:
            df = pd.read_csv(path.join(self.gtfs_folder, name))
            df = df.applymap(str)
            dicts = df.to_dict(orient="records")
            objects = list(map(lambda x: model(**x), dicts))
            session = self.rdb_provider.get_session()
            try:
                # Try to save bulks (initial load)
                chunks = [objects[i:i + 100]
                          for i in range(0, len(objects), 100)]
                for chunk in chunks:
                    session.bulk_save_objects(chunk)
                    session.commit()
            except Exception:
                # Or save items one after the other
                session.rollback()
                for obj in objects:
                    merged_obj = session.merge(obj)
                    session.commit()


class ScheduleExtractorDynamo(ScheduleExtractor):
    """ For dynamo
    """

    def __init__(self, yyyymmdd, data_folder=None, schedule_url=None, dynamo_table=None, read_on=None, write_on=None, read_off=None, write_off=None):

        ScheduleExtractor.__init__(
            self, data_folder=data_folder, schedule_url=schedule_url)

        # Check dates formats (list or single entry)
        if isinstance(yyyymmdd, str) or isinstance(yyyymmdd, int):
            yyyymmdd = [str(yyyymmdd)]
        if isinstance(yyyymmdd, list):
            for yyyymmdd_day in yyyymmdd:
                try:
                    datetime.strptime(yyyymmdd_day, "%Y%m%d")
                except ValueError as e:
                    raise ValueError(
                        "Error: date in wrong format, should be yyyymmdd")
        self.days = yyyymmdd

        # Data to be collected
        self.services = {}
        self.trips = {}
        self.departures = {}

        # Dynamo ORM
        self.dynamo_departures = {}

        # Dynamo saving status
        self.dynamo_saved_days = []

        # Dynamo table
        if not dynamo_table:
            dynamo_table = dynamo_sched_dep
        self.dynamo_table = dynamo_table

        # ProvisionedThroughput
        if not read_on:
            read_on = shed_read
        self.read_on = read_on

        if not read_off:
            read_off = shed_read
        self.read_off = read_off

        if not write_on:
            write_on = shed_write_on
        self.write_on = write_on

        if not write_off:
            write_off = shed_write_off
        self.write_off = write_off

        if self.files_present:
            self.build_all_departures()
        else:
            logging.warning(
                "Files were not present. Please download gtfs files and call build_all_departures method before trying to save schedule in dynamo.")

    def _services_of_day(self, yyyymmdd):
        """
        Given a date, this function will return all service-ids scheduled on transilien's network on this day.

        This function requires that gtfs files are present in data folder specified in settings module.

        :param yyyymmdd: date on yyyymmdd format
        :type yyyymmdd: string or int

        :rtype: list
        """
        # Get weekday: for double check
        datetime_format = datetime.strptime(yyyymmdd, "%Y%m%d")
        weekday = calendar.day_name[datetime_format.weekday()].lower()

        all_services = pd.read_csv(path.join(self.gtfs_folder, "calendar.txt"))

        cond1 = all_services[weekday] == 1
        cond2 = all_services["start_date"] <= int(yyyymmdd)
        cond3 = all_services["end_date"] >= int(yyyymmdd)
        all_services = all_services[cond1 & cond2 & cond3]["service_id"].values

        # Get service exceptions
        # 1 = service (alors que normalement non)
        # 2 = pas service (alors que normalement oui)
        serv_exc = pd.read_csv(
            path.join(self.gtfs_folder, "calendar_dates.txt"))
        serv_exc = serv_exc[serv_exc["date"] == int(yyyymmdd)]

        serv_add = serv_exc[serv_exc["exception_type"] == 1][
            "service_id"].values
        serv_rem = serv_exc[serv_exc["exception_type"] == 2][
            "service_id"].values

        serv_on_day = set(all_services)
        serv_on_day.update(serv_add)
        serv_on_day = serv_on_day - set(serv_rem)
        serv_on_day = list(serv_on_day)

        self.services[yyyymmdd] = serv_on_day

        return serv_on_day

    def _trips_of_day(self, yyyymmdd):
        """
        Given a date, this function will return all trip-ids scheduled on transilien's network on this day.

        This function requires that gtfs files are present in data folder specified in settings module.

        :param yyyymmdd: date on yyyymmdd format
        :type yyyymmdd: string or int

        :rtype: list
        """
        all_trips = pd.read_csv(path.join(self.gtfs_folder, "trips.txt"))
        services_on_day = self._services_of_day(yyyymmdd)
        trips_condition = all_trips["service_id"].isin(services_on_day)
        trips_on_day = list(all_trips[trips_condition]["trip_id"].unique())

        self.trips[yyyymmdd] = trips_on_day

        return trips_on_day

    def _departures_of_day(self, yyyymmdd, stop_filter=None, station_filter=None, return_df=False, dropna_index=["station_id", "day_train_num"]):
        """
        Given a date, this function will return all trip-ids scheduled on transilien's network on this day.

        This function requires that gtfs files are present in data folder specified in settings module.

        :param yyyymmdd: date on yyyymmdd format
        :type yyyymmdd: string or int

        :param stop_filter: default None. If set, should be a list of stops for which you want to obtain stop times scheduled on this day. Otherwise, if set to False or None, it will get all stops without restrictions on stations.
        :type stop_filter: None/False or list of valid stops_ids

        :param station_filter: default None. If set, should be a list of station ids for which you want to obtain stop times scheduled on this day. Otherwise, if set to False or None, it will get all stops without restrictions on stations.
        :type station_filter: None/False or list of valid station_filter

        :param df_format: default False. If set to True, will return a pandas dataframe
        :type df_format: boolean

        :param dropna_index: default ["station_id", "day_train_num"]. If set, it will drop all rows where the index fields might have NaN values or are duplicates of others rows.
        :type dropna_index: list or None

        :rtype: list of json serializable objects, or pandas dataframe if df_format is set to True
        """

        all_stop_times = pd.read_csv(
            path.join(self.gtfs_folder, "stop_times.txt"))

        # Take either all lines, either only those for given day
        if yyyymmdd == "all":
            matching_stop_times = all_stop_times
        else:
            trips_on_day = self._trips_of_day(yyyymmdd)
            cond1 = all_stop_times["trip_id"].isin(trips_on_day)
            matching_stop_times = all_stop_times[cond1]
        # Add trips routes and agency fields
        # agency = pd.read_csv(path.join(gtfs_path, "agency.txt"))
        # agency = agency[["agency_id", "agency_name"]]
        routes = pd.read_csv(path.join(self.gtfs_folder, "routes.txt"))
        routes = routes[["route_id", "route_short_name"]]
        # routes = routes.merge(agency, on="agency_id", how="inner")
        trips = pd.read_csv(path.join(self.gtfs_folder, "trips.txt"))
        trips = trips.merge(routes, on="route_id", how="inner")

        matching_stop_times = matching_stop_times.merge(
            trips, on="trip_id", how="inner")

        # Custom fields
        matching_stop_times.loc[:, "scheduled_departure_day"] = yyyymmdd
        matching_stop_times.rename(
            columns={'departure_time': 'scheduled_departure_time'}, inplace=True)
        matching_stop_times.loc[:, "station_id"] = matching_stop_times[
            "stop_id"].str.extract("DUA(\d{7})", expand=False)
        matching_stop_times.loc[:, "train_num"] = matching_stop_times[
            "trip_id"].str.extract("^.{5}(\d{6})", expand=False)
        # Dynamo sort key (hash is station_id)
        matching_stop_times.loc[:, "day_train_num"] = matching_stop_times.apply(
            lambda x: "%s_%s" % (x["scheduled_departure_day"], x["train_num"]), axis=1)

        # Drop na and dups on indexes: station_id and day_train_num
        if isinstance(dropna_index, list):
            matching_stop_times = matching_stop_times.dropna(
                subset=dropna_index)
            matching_stop_times = matching_stop_times.drop_duplicates(
                subset=dropna_index)

        # Station filtering if asked
        if stop_filter:
            cond2 = matching_stop_times["stop_id"].isin(stop_filter)
            matching_stop_times = matching_stop_times[cond2]

        if station_filter:
            cond3 = matching_stop_times["station_id"].isin(station_filter)
            matching_stop_times = matching_stop_times[cond3]

        # All saved as str in dynamo
        matching_stop_times = matching_stop_times.applymap(str)

        dict_list = matching_stop_times.to_dict(orient='records')
        logger.info("There are %d scheduled departures on %s",
                    len(dict_list), yyyymmdd)
        self.departures[yyyymmdd] = dict_list

        # Prepare objects for dynamo saving
        objects = [ScheduledDeparture(**item) for item in dict_list]
        self.dynamo_departures[yyyymmdd] = objects

        if return_df:
            return matching_stop_times

    def build_all_departures(self):
        assert self.files_present

        for yyyymmdd_day in self.days:
            self._departures_of_day(yyyymmdd_day)

    def _dynamo_save_departures_of_day(self, yyyymmdd):
        """
        Given a date, this function will save all trip-ids scheduled on transilien's network on this day in Dynamo's 'scheduled_departures' table.

        :param yyyymmdd: date on yyyymmdd format
        :type yyyymmdd: string or int
        """
        objects = self.dynamo_departures[yyyymmdd]

        with ScheduledDeparture.batch_write() as batch:
            for obj in objects:
                batch.save(obj)

        self.dynamo_saved_days.append(yyyymmdd)

    def dynamo_save_departures(self):
        for yyyymmdd in self.days:
            self._dynamo_save_departures_of_day(yyyymmdd)

    def _adapt_table_provision(self, read, write):
        try:
            dynamo_update_provisionned_capacity(
                read=read, write=write, table_name=self.dynamo_table)
        except Exception as e:
            logger.warning("Could not change provisioned_throughput %s", e)

    def dynamo_save_departures_and_provision(self, low_afterwards=True):
        self._adapt_table_provision(read=self.read_on, write=self.write_on)
        # Wait for one minute till provisioned_throughput is updated
        time.sleep(60)

        self.dynamo_save_departures()

        if low_afterwards:
            # Reset provisioned_throughput to minimal writing
            self._adapt_table_provision(self.read_off, self.write_off)
