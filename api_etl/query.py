"""
Module used to query schedule data contained in relational databases.
"""

import logging
from datetime import datetime

# from sqlalchemy.orm import aliased
from sqlalchemy.sql import func

from api_etl.utils_misc import get_paris_local_datetime_now
from api_etl.utils_rdb import RdbProvider
from api_etl.models import (
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route,
    RealTimeDeparture
)


class DBQuerier():
    """ This class allows you to easily query information available in
    databases: both RDB containing schedules, and Dynamo DB containing
    real-time data.
    \nThe possible methods are:
    \n -services_of_day: returns a list of strings.
    \n -trip_stops: gives trips stops for a given trip_id.
    \n -station_trip_stops: gives trips stops for a given station_id (in gtfs
    format:7 digits).
    """

    def __init__(self, yyyymmdd=None):
        self.provider = RdbProvider()
        if not yyyymmdd:
            yyyymmdd = get_paris_local_datetime_now().strftime("%Y%m%d")
        else:
            # Will raise an error if wrong format
            datetime.strptime(yyyymmdd, "%Y%m%d")
        self.yyyymmdd = yyyymmdd

    def set_date(self, yyyymmdd):
        """Sets date that will define default date for requests.
        """
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")
        self.yyyymmdd = yyyymmdd

    def routes(self):
        results = self.provider.get_session()\
            .query(Route)\
            .distinct(Route.route_short_name)\
            .all()
        return ResultSetSerializer(results)

    def stations(self, on_route_short_name=None):
        """
        Return list of stations.
        You can specify filter on given route.

        Stop -> StopTime -> Trip -> Route
        """
        if on_route_short_name:
            on_route_short_name = str(on_route_short_name)

        results = self.provider.get_session()\
            .query(Stop)

        if on_route_short_name:
            results = results\
                .filter(Stop.stop_id == StopTime.stop_id)\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(Trip.route_id == Route.route_id)\
                .filter(Route.route_short_name == on_route_short_name)\

        # Distinct, and only stop points (stop area are duplicates
        # of stop points)
        results = results.distinct(Stop.stop_id)\
            .filter(Stop.stop_id.like("StopPoint%"))\
            .all()

        return results

    def services_of_day(self, yyyymmdd=None):
        """Return all service_id's for a given day.
        """
        yyyymmdd = yyyymmdd or self.yyyymmdd
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")

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
        serv_on_day = list(map(lambda x: x[0], serv_on_day))

        return serv_on_day

    def trips_of_day(
        self, yyyymmdd=None, active_at_time=None, has_begun_at_time=None, not_yet_arrived_at_time=None, trip_id_only=True
    ):
        """Returns list of strings (trip_ids).
        Day is either specified or today.

        Possible filters:
        - active_at_time: "hh:mm:ss" (if set only to boolean True, "now")
        - has_begun_at_time
        - not_yet_arrived_at_time
        """

        # Args parsing:
        yyyymmdd = yyyymmdd or self.yyyymmdd
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")

        # if active_at_time is set to boolean True, takes now
        if active_at_time is True:
            active_at_time = get_paris_local_datetime_now()\
                .strftime("%H:%M:%S")

        # active_at is set if other args are None
        has_begun_at_time = has_begun_at_time or active_at_time
        not_yet_arrived_at_time = not_yet_arrived_at_time or active_at_time

        # session init
        session = self.provider.get_session()

        if not has_begun_at_time and not not_yet_arrived_at_time:
            # Case where no constraint
            results = session\
                .query(Trip.trip_id if trip_id_only else Trip)\
                .filter(Trip.service_id.in_(self.services_of_day(yyyymmdd)))\
                .all()
            return list(map(lambda x: x[0], results))

        if has_begun_at_time:
            # Begin constraint: "hh:mm:ss" up to 26 hours
            # trips having begun at time:
            # => first stop departure_time must be < time
            results = session\
                .query(Trip.trip_id)\
                .filter(Trip.service_id.in_(self.services_of_day(yyyymmdd)))\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(StopTime.stop_sequence == "0")\
                .filter(StopTime.departure_time <= has_begun_at_time)\
                .all()
            begin_results = list(map(lambda x: x[0], results))

        if not_yet_arrived_at_time:
            # End constraint: trips not arrived at time
            # => last stop departure_time must be > time
            results = session\
                .query(Trip.trip_id)\
                .filter(Trip.service_id == Calendar.service_id)\
                .filter(Trip.service_id.in_(self.services_of_day(yyyymmdd)))\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(
                    StopTime.stop_sequence == session
                    .query(func.max(StopTime.stop_sequence))
                    .correlate(Trip)
                )\
                .filter(StopTime.departure_time >= not_yet_arrived_at_time)\
                .all()
            end_results = list(map(lambda x: x[0], results))

        if not_yet_arrived_at_time and has_begun_at_time:
            trip_ids_result = list(
                set(begin_results).intersection(end_results))

        elif has_begun_at_time:
            trip_ids_result = begin_results

        elif not_yet_arrived_at_time:
            trip_ids_result = end_results

        if trip_id_only:
            return trip_ids_result
        else:
            result = session\
                .query(Trip)\
                .filter(Trip.trip_id.in_(trip_ids_result))\
                .all()
            return result

    def stoptimes_of_day(self, yyyymmdd, stops_only=False):
        if stops_only:
            results = self.provider.get_session()\
                .query(StopTime)\
                .filter(StopTime.trip_id.in_(self.trips_of_day(yyyymmdd)))\
                .all()
            return results
        else:
            results = self.provider.get_session()\
                .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
                .filter(Trip.trip_id == StopTime.trip_id)\
                .filter(Stop.stop_id == StopTime.stop_id)\
                .filter(Trip.route_id == Route.route_id)\
                .filter(Agency.agency_id == Route.agency_id)\
                .filter(Calendar.service_id == Trip.service_id)\
                .filter(StopTime.trip_id.in_(self.trips_of_day(yyyymmdd)))\
                .all()
            return results

    def trip_stoptimes(self, trip_id, yyyymmdd=None):
        """Return all stops of a given trip, on a given day.
        \nInitially the returned object contains only schedule information.
        \nThen, it can be updated with realtime information.
        """
        yyyymmdd = yyyymmdd or self.yyyymmdd
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Trip.trip_id == trip_id)\
            .all()
        return results

    def station_stoptimes(self, uic_code, yyyymmdd=None):
        """Return all trip stops of a given station, on a given day.
        \n -uic_code should be in 7 or 8 digits gtfs format
        \n -day is in yyyymmdd format
        \n
        \nInitially the returned object contains only schedule information.
        \nThen, it can be updated with realtime information.
        """
        yyyymmdd = yyyymmdd or self.yyyymmdd
        # Will raise error if wrong format
        datetime.strptime(yyyymmdd, "%Y%m%d")

        uic_code = str(uic_code)
        assert ((len(uic_code) == 7) or (len(uic_code) == 8))
        if len(uic_code) == 8:
            uic_code = uic_code[:-1]

        results = self.provider.get_session()\
            .query(StopTime, Trip, Stop, Route, Agency, Calendar)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.route_id == Route.route_id)\
            .filter(Agency.agency_id == Route.agency_id)\
            .filter(StopTime.stop_id.like("%" + uic_code))\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Calendar.service_id.in_(self.services_of_day(yyyymmdd)))\
            .all()

        return results
