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
    \n -services_on_day: returns a list of strings.
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

    def routes(self, distinct_short_name=True, level=0, limit=None):
        """ Multiple options available.

        Entity levels:
        - 0: only ids
        - 1: only routes
        - 2: routes, agencies
        """
        # ARGS PARSING

        # entities
        if level == 0:
            entities = [Route.route_id]
        elif level == 1:
            entities = [Route]
        elif level == 2:
            entities = [Route, Agency]
        else:
            entities = [Route.route_id]

        # Limit
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = False

        # QUERY
        session = self.provider.get_session()
        results = session\
            .query(*entities)\
            .filter(Agency.agency_id == Route.agency_id)\
            .distinct(
                Route.route_short_name if distinct_short_name
                else Route.route_id
            )

        if limit:
            results = results.limit(limit)

        return results.all()

    def stations(self, on_route_short_name=None, level=0, limit=None):
        """
        Return list of stations.
        You can specify filter on given route.
        Stop -> StopTime -> Trip -> Route

        Entity levels:
        - 0: only ids
        - 1: Stop
        """

        # ARGS PARSING
        # entities
        if level == 0:
            entities = [Stop.stop_id]
        elif level == 1:
            entities = [Stop]
        else:
            entities = [Stop]

        if on_route_short_name:
            on_route_short_name = str(on_route_short_name)

        # Limit
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = False

        # QUERY
        session = self.provider.get_session()
        results = session.query(*entities)

        if on_route_short_name:
            # Stop -> StopTime -> Trip -> Route
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

        if limit:
            results = results.limit(limit)

        return results

    def services(self, on_day=None, level=0, limit=None):
        """
        Return services.

        Filter:
        - of day

        Entity levels:
        - 0: only ids
        - 1: Service (Calendar)
        """
        # ARGS PARSING
        # on_day
        if on_day is True:
            on_day = self.yyyymmdd
        if on_day:
            # Will raise error if wrong format
            datetime.strptime(on_day, "%Y%m%d")

        # entities
        if level == 0:
            entities = [Calendar.service_id]
        elif level == 1:
            entities = [Calendar]
        else:
            entities = [Calendar]

        # Limit
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = False

        # QUERY
        session = self.provider.get_session()

        # Query if no day filter
        if not on_day:
            result = session\
                .query(*entities)
            return result.all()

        # Query if day filter
        serv_regular = session\
            .query(*entities)\
            .filter(Calendar.start_date <= on_day)\
            .filter(Calendar.end_date >= on_day)

        # Get service exceptions
        # 1 = service (instead of usually not)
        # 2 = no service (instead of usually yes)

        serv_add = session\
            .query(*entities)\
            .filter(CalendarDate.service_id == Calendar.service_id)\
            .filter(CalendarDate.date == on_day)\
            .filter(CalendarDate.exception_type == "1")

        serv_rem = session\
            .query(*entities)\
            .filter(CalendarDate.service_id == Calendar.service_id)\
            .filter(CalendarDate.date == on_day)\
            .filter(CalendarDate.exception_type == "2")

        results = serv_regular.union(serv_add).except_(serv_rem)

        if limit:
            results = results.limit(limit)

        return results.all()

    def trips(
        self, on_day=None, active_at_time=None, has_begun_at_time=None,
        not_yet_arrived_at_time=None, level=0, limit=None
    ):
        """Returns list of strings (trip_ids).
        Day is either specified or today.

        Possible filters:
        - active_at_time: "hh:mm:ss" (if set only to boolean True, time "now")
        - has_begun_at_time
        - not_yet_arrived_at_time

        Entity levels:
        - 0: only ids
        - 1: Trip
        - 2: Trip, Calendar
        - 3: Trip, Calendar, Route
        - 4: Trip, Calendar, Route, Agency
        """
        # ARGS PARSING
        # on_day:
        if on_day is True:
            on_day = self.yyyymmdd

        if on_day:
            # Will raise error if wrong format
            datetime.strptime(on_day, "%Y%m%d")

        # if active_at_time is set to boolean True, takes now
        if active_at_time is True:
            active_at_time = get_paris_local_datetime_now()\
                .strftime("%H:%M:%S")

        # active_at is set if other args are None
        has_begun_at_time = has_begun_at_time or active_at_time
        not_yet_arrived_at_time = not_yet_arrived_at_time or active_at_time

        # entities
        if level == 0:
            entities = [Trip.trip_id]
        elif level == 1:
            entities = [Trip]
        elif level == 2:
            entities = [Trip, Calendar]
        elif level == 3:
            entities = [Trip, Calendar, Route]
        elif level == 4:
            entities = [Trip, Calendar, Route, Agency]
        else:
            entities = [Trip]

        # Limit
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = False

        # QUERY
        # session init
        session = self.provider.get_session()

        # All trips
        results = session.query(*entities)

        if on_day:
            results = results\
                .filter(Trip.service_id.in_(self.services(on_day=on_day)))

        if has_begun_at_time:
            # Begin constraint: "hh:mm:ss" up to 26 hours
            # trips having begun at time:
            # => first stop departure_time must be < time
            begin_results = session\
                .query(*entities)\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(StopTime.stop_sequence == "0")\
                .filter(StopTime.departure_time <= has_begun_at_time)\

            results = results.intersect(begin_results)

        if not_yet_arrived_at_time:
            # End constraint: trips not arrived at time
            # => last stop departure_time must be > time
            end_results = session\
                .query(*entities)\
                .filter(StopTime.trip_id == Trip.trip_id)\
                .filter(
                    StopTime.stop_sequence == session
                    .query(func.max(StopTime.stop_sequence))
                    .correlate(Trip)
                )\
                .filter(StopTime.departure_time >= not_yet_arrived_at_time)

            results = results.intersect(end_results)

        if limit:
            results = results.limit(limit)

        return results.all()

    def stoptimes(
        self, on_day=None, trip_id_filter=None, uic_filter=None,
        trip_active_at_time=None, level=0, limit=None
    ):
        """ Returns stoptimes

        Uic filter accepts both 7 and 8 digits, but only one station.

        Entity levels:
        - 0: stoptime (stop and trip) ids
        - 1: only stoptimes
        - 2: stoptimes, trips
        - 3: stoptimes, trips, stops
        - 4: stoptimes, trips, stops, routes, calendar
        """
        # ARGS PARSING
        # on_day
        if on_day is True:
            on_day = self.yyyymmdd
        if on_day:
            # Will raise error if wrong format
            datetime.strptime(on_day, "%Y%m%d")

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
            entities = [StopTime.stop_id, StopTime.trip_id]
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

        # Limit
        try:
            limit = int(limit)
        except (ValueError, TypeError):
            limit = False

        # QUERY
        session = self.provider.get_session()

        # Filters for joins (no effect if level is lower)
        results = session\
            .query(*entities)\
            .filter(Stop.stop_id == StopTime.stop_id)\
            .filter(Trip.trip_id == StopTime.trip_id)\
            .filter(Calendar.service_id == Trip.service_id)\
            .filter(Route.route_id == Trip.route_id)\
            .filter(Agency.agency_id == Route.agency_id)

        if on_day:
            results = results\
                .filter(Trip.service_id.in_(self.services(on_day=on_day)))

        if trip_active_at_time:
            results = results.filter(
                Trip.trip_id.in_(
                    self.trips(
                        on_day=on_day,
                        active_at_time=trip_active_at_time
                    )
                ))

        if trip_id_filter:
            # accepts list or single element
            if not isinstance(trip_id_filter, list):
                trip_id_filter = [trip_id_filter]
            # filter
            results = results\
                .filter(Trip.trip_id.in_(trip_id_filter))

        if uic_filter:
            results = results\
                .filter(Stop.stop_id.like("%"))\

        if limit:
            results = results.limit(limit)

        return results.all()
