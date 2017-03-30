"""
Module used to query schedule data contained in relational databases.
"""

import logging

import pandas as pd

from api_etl.utils_misc import get_paris_local_datetime_now
from api_etl.utils_rdb import Provider, ResultSerializer
from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route
)

logger = logging.getLogger(__name__)
pd.options.mode.chained_assignment = None


class RdbQuerier():

    def __init__(self):
        self.provider = Provider()

    def services_of_day(self, yyyymmdd=None):
        if not yyyymmdd:
            yyyymmdd = get_paris_local_datetime_now().strftime("%Y%m%d")

        yyyymmdd = str(yyyymmdd)
        assert len(yyyymmdd) == 8
        all_services = self.provider.get_session()\
            .query(Calendar.service_id)\
            .filter(Calendar.start_date <= yyyymmdd)\
            .filter(Calendar.end_date >= yyyymmdd)\
            .all()

        # Get service exceptions
        # 1 = service (instead of usually not)
        # 2 = no service (instead of usually yes)

        serv_add = self.provider.get_session()\
            .query(CalendarDate.service_id)\
            .filter(CalendarDate.date == yyyymmdd)\
            .filter(CalendarDate.exception_type == "1")\
            .all()

        serv_rem = self.provider.get_session()\
            .query(CalendarDate.service_id)\
            .filter(CalendarDate.date == yyyymmdd)\
            .filter(CalendarDate.exception_type == "2")\
            .all()

        serv_on_day = set(all_services)
        serv_on_day.update(serv_add)
        serv_on_day = serv_on_day - set(serv_rem)
        serv_on_day = map(lambda x: x[0], serv_on_day)
        serv_on_day = list(serv_on_day)

        return serv_on_day

    def trip_stops(self, trip_id):
        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(Trip.trip_id == trip_id)\
            .all()
        return ResultSerializer(results)

    def station_trips_stops(self, station_id, yyyymmdd=None):
        """ station_id should be in 7 digits gtfs format
        """
        station_id = str(station_id)
        assert len(station_id) == 7

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(StopTime.stop_id.match(station_id))\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Calendar.service_id.in_(self.services_of_day(yyyymmdd)))\
            .all()

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(StopTime.stop_id.like("%" + station_id))\
            .filter(Trip.service_id.in_(self.services_of_day(yyyymmdd)))\
            .all()
        #

        return ResultSerializer(results)
