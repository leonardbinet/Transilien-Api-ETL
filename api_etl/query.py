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
    Calendar, CalendarDate, Trip, StopTime, Stop, Agency, Route
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

    def routes(self, distinct_short_name=True, ids_only=True):
        """ Multiple options available.
        """
        session = self.provider.get_session()
        results = session\
            .query(Route.route_id if ids_only else Route)\
            .distinct(
                Route.route_short_name if distinct_short_name
                else Route.route_id
            )\
            .all()
        return results

    def stations(self, on_route_short_name=None, ids_only=True):
        """
        Return list of stations.
        You can specify filter on given route.

        Stop -> StopTime -> Trip -> Route
        """
        if on_route_short_name:
            on_route_short_name = str(on_route_short_name)

        session = self.provider.get_session()
        results = session.query(Stop.stop_id if ids_only else Stop)

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

    def services(self, of_day=None, ids_only=True):
        """Return all service_id's for a given day.
        """
        # Args parsing of of_day
        if of_day is True:
            of_day = self.yyyymmdd
        if of_day:
            # Will raise error if wrong format
            datetime.strptime(of_day, "%Y%m%d")

        # ids_only
        entity = Calendar.service_id if ids_only else Calendar

        # Session init
        session = self.provider.get_session()

        # Query if no day filter
        if not of_day:
            result = session\
                .query(entity)\
                .all()
            return result

        # Query if day filter
        serv_regular = session\
            .query(entity)\
            .filter(Calendar.start_date <= of_day)\
            .filter(Calendar.end_date >= of_day)

        # Get service exceptions
        # 1 = service (instead of usually not)
        # 2 = no service (instead of usually yes)

        serv_add = session\
            .query(entity)\
            .filter(CalendarDate.service_id == Calendar.service_id)\
            .filter(CalendarDate.date == of_day)\
            .filter(CalendarDate.exception_type == "1")

        serv_rem = session\
            .query(entity)\
            .filter(CalendarDate.service_id == Calendar.service_id)\
            .filter(CalendarDate.date == of_day)\
            .filter(CalendarDate.exception_type == "2")

        serv_on_day = serv_regular.union(serv_add).except_(serv_rem).all()
        return serv_on_day

    def trips(
        self, of_day=None, active_at_time=None, has_begun_at_time=None,
        not_yet_arrived_at_time=None, ids_only=True
    ):
        """Returns list of strings (trip_ids).
        Day is either specified or today.

        Possible filters:
        - active_at_time: "hh:mm:ss" (if set only to boolean True, time "now")
        - has_begun_at_time
        - not_yet_arrived_at_time
        """
        # ARGS PARSING
        # of_day:
        if of_day is True:
            of_day = self.yyyymmdd

        if of_day:
            # Will raise error if wrong format
            datetime.strptime(of_day, "%Y%m%d")

        # if active_at_time is set to boolean True, takes now
        if active_at_time is True:
            active_at_time = get_paris_local_datetime_now()\
                .strftime("%H:%M:%S")

        # active_at is set if other args are None
        has_begun_at_time = has_begun_at_time or active_at_time
        not_yet_arrived_at_time = not_yet_arrived_at_time or active_at_time

        # ids_only
        entities = Trip.trip_id if ids_only else Trip

        # QUERY
        # session init
        session = self.provider.get_session()

        # All trips
        results = session.query(entities)

        # If constraint on day: filter on them
        if of_day:
            results = results\
                .filter(Trip.service_id.in_(self.services(of_day=of_day)))

        if has_begun_at_time:
            # Begin constraint: "hh:mm:ss" up to 26 hours
            # trips having begun at time:
            # => first stop departure_time must be < time
            begin_results = session\
                .query(entities)\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(StopTime.stop_sequence == "0")\
                .filter(StopTime.departure_time <= has_begun_at_time)\

            results = results.intersect(begin_results)

        # Else
        if not_yet_arrived_at_time:
            # End constraint: trips not arrived at time
            # => last stop departure_time must be > time
            end_results = session\
                .query(entities)\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(
                    StopTime.stop_sequence == session
                    .query(func.max(StopTime.stop_sequence))
                    .correlate(Trip)
                )\
                .filter(StopTime.departure_time >= not_yet_arrived_at_time)

            results = results.intersect(end_results)

        return results.all()

    def stoptimes(
        self, of_day=None, level=0, trip_id_filter=None, uic_filter=None,
        trip_active_at_time=None
    ):
        """ Returns stoptimes

        Uic filter accepts both 7 and 8 digits, but only one station.

        Entity levels:
        - 0: only ids
        - 1: only stoptimes
        - 2: stoptimes, trips
        - 3: stoptimes, trips, stops
        - 4: stoptimes, trips, stops, routes, calendar
        """
        # ARGS PARSING
        # of_day
        if of_day is True:
            of_day = self.yyyymmdd
        if of_day:
            # Will raise error if wrong format
            datetime.strptime(of_day, "%Y%m%d")

        # uic_filter
        if uic_filter:
            uic_filter = str(uic_filter)
            if len(uic_filter) == 8:
                uic_filter = uic_filter[:-1]
            elif len(uic_filter) == 7:
                pass
            else:
                raise ValueError("uic_filter length must be 7 or 8")

        # entities
        if level == 0:
            entities = [StopTime.stop_id]
        elif level == 1:
            entities = [StopTime]
        elif level == 2:
            entities = [StopTime, Trip]
        elif level == 3:
            entities = [StopTime, Trip, Stop]
        elif level == 4:
            entities = [StopTime, Trip, Stop, Route, Calendar]
        else:
            entities = [StopTime.stop_id]

        # Session init
        session = self.provider.get_session()

        # Filters for joins (no effect if level is lower)
        results = session\
            .query(*entities)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Route.route_id == Trip.route_id)\
            .filter(Agency.agency_id == Route.agency_id)

        if of_day:
            results = results\
                .filter(Trip.service_id.in_(self.services(of_day=of_day)))

        if trip_active_at_time:
            results = results\
                .filter(Trip.trip_id.in_(
                    self.trips(
                        of_day=of_day,
                        active_at_time=trip_active_at_time
                    )
                ))

        if trip_id_filter:
            # accepts list or single element
            if not isinstance(trip_id_filter):
                trip_id_filter = [trip_id_filter]
            # filter
            results = results\
                .filter(Trip.trip_id.in_(trip_id_filter))

        if uic_filter:
            results = results\
                .filter(Stop.stop_id.like("%"))\

        return results.all()
