""" Data Models for databases: relational databases, and Dynamo.
"""

from pynamodb.models import Model as DyModel
from pynamodb.attributes import UnicodeAttribute

from sqlalchemy.ext import declarative
from sqlalchemy import Column, String, ForeignKey

from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter
from api_etl.utils_secrets import get_secret
from api_etl.settings import __DYNAMO_REALTIME__

# Set as environment variable: boto takes it directly
AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)

RdbModel = declarative.declarative_base()


class RealTimeDeparture(DyModel):

    class Meta:
        table_name = __DYNAMO_REALTIME__["name"]
        region = AWS_DEFAULT_REGION

    # Raw data from API
    date = UnicodeAttribute()
    station_8d = UnicodeAttribute()
    train_num = UnicodeAttribute()
    miss = UnicodeAttribute(null=True)
    term = UnicodeAttribute()
    etat = UnicodeAttribute(null=True)

    # Fields added for indexing and identification
    day_train_num = UnicodeAttribute(range_key=True)
    station_id = UnicodeAttribute(hash_key=True)

    # Custom time fields
    # Expected passage day and time are 'weird' dates -> to 27h
    expected_passage_day = UnicodeAttribute()
    expected_passage_time = UnicodeAttribute()
    request_day = UnicodeAttribute()
    request_time = UnicodeAttribute()
    data_freshness = UnicodeAttribute()

    def __repr__(self):
        return "<RealTime(train_num='%s', station_8d='%s', expected_passage_day='%s', data_freshness='%s')>"\
            % (self.train_num, self.station_8d, self.expected_passage_day, self.data_freshness)

    def __str__(self):
        return self.__repr__()

    def _has_passed(self, at_datetime=None, seconds=False):
        """ Checks if train expected passage time has passed, compared to a
        given datetime. If none provided, compared to now.
        """
        if not at_datetime:
            at_datetime = get_paris_local_datetime_now().replace(tzinfo=None)

        dt = self.expected_passage_time
        dd = self.expected_passage_day

        time_past_dep = DateConverter(dt=at_datetime)\
            .compute_delay_from(special_date=dd, special_time=dt)

        if seconds:
            # return number of seconds instead of boolean
            return time_past_dep

        return time_past_dep >= 0


class Agency(RdbModel):
    __tablename__ = 'agencies'

    agency_id = Column(String(50), primary_key=True)
    agency_name = Column(String(50))
    agency_url = Column(String(50))
    agency_timezone = Column(String(50))
    agency_lang = Column(String(50))

    def __repr__(self):
        return "<Agency(agency_id='%s', agency_name='%s', agency_url='%s')>"\
            % (self.agency_id, self.agency_name, self.agency_url)

    def __str__(self):
        return self.__repr__()


class Route(RdbModel):
    __tablename__ = 'routes'

    route_id = Column(String(50), primary_key=True)
    agency_id = Column(String(50), ForeignKey('agencies.agency_id'))
    route_short_name = Column(String(50))
    route_long_name = Column(String(100))
    route_desc = Column(String(150))
    route_type = Column(String(50))
    route_url = Column(String(50))
    route_color = Column(String(50))
    route_text_color = Column(String(50))

    def __repr__(self):
        return "<Route(route_id='%s', route_short_name='%s', route_long_name='%s')>"\
            % (self.route_id, self.route_short_name, self.route_long_name)

    def __str__(self):
        return self.__repr__()


class Trip(RdbModel):
    __tablename__ = 'trips'

    trip_id = Column(String(50), primary_key=True)
    route_id = Column(String(50), ForeignKey('routes.route_id'))
    service_id = Column(String(50))
    trip_headsign = Column(String(50))
    direction_id = Column(String(50))
    block_id = Column(String(50))

    def __repr__(self):
        return "<Trip(trip_id='%s', route_id='%s', trip_headsign='%s')>"\
            % (self.trip_id, self.route_id, self.trip_headsign)

    def __str__(self):
        return self.__repr__()


class StopTime(RdbModel):
    __tablename__ = 'stop_times'
    trip_id = Column(String(50), ForeignKey('trips.trip_id'), primary_key=True)
    stop_id = Column(String(50), ForeignKey('stops.stop_id'), primary_key=True)

    arrival_time = Column(String(50))
    departure_time = Column(String(50))
    stop_sequence = Column(String(50))
    stop_headsign = Column(String(50))
    pickup_type = Column(String(50))
    drop_off_type = Column(String(50))

    def __repr__(self):
        return "<StopTime(trip_id='%s', stop_id='%s', stop_sequence='%s')>"\
            % (self.trip_id, self.stop_id, self.stop_sequence)

    def __str__(self):
        return self.__repr__()

    def _get_partial_index(self):
        self._station_id = self.stop_id[-7:]
        self._train_num = self.trip_id[5:11]
        return self._station_id, self._train_num

    def _get_realtime_index(self, scheduled_day):
        self._get_partial_index()
        self._scheduled_day = scheduled_day
        self._day_train_num = "%s_%s" % (scheduled_day, self._train_num)
        return self._station_id, self._day_train_num

    def _has_passed(self, at_datetime=None, seconds=False):
        """ Checks if train expected passage time has passed, based on:
        - expected_passage_time we got from realtime api.
        - scheduled_departure_time from gtfs
        And sets it as attributes.
        """

        if not hasattr(self, "_scheduled_day"):
            # if realtime query not made
            self._scheduled_day = at_datetime.strftime("%Y%m%d")

        if not at_datetime:
            at_datetime = get_paris_local_datetime_now().replace(tzinfo=None)

        dt = self.departure_time
        # either provided, either one saved through previous realtime request
        dd = self._scheduled_day

        time_past_dep = DateConverter(dt=at_datetime)\
            .compute_delay_from(special_date=dd, special_time=dt)

        if seconds:
            # return number of seconds instead of boolean
            return time_past_dep

        return time_past_dep >= 0


class Stop(RdbModel):
    __tablename__ = 'stops'

    stop_id = Column(String(50), primary_key=True)
    stop_name = Column(String(150))
    stop_desc = Column(String(150))
    stop_lat = Column(String(50))
    stop_lon = Column(String(50))
    zone_id = Column(String(50))
    stop_url = Column(String(50))
    location_type = Column(String(50))
    parent_station = Column(String(50))

    def __repr__(self):
        return "<Stop(stop_id='%s', stop_name='%s')>"\
            % (self.stop_id, self.stop_name)

    def __str__(self):
        return self.__repr__()


class Calendar(RdbModel):
    __tablename__ = 'calendars'

    service_id = Column(String, primary_key=True)
    monday = Column(String(50))
    tuesday = Column(String(50))
    wednesday = Column(String(50))
    thursday = Column(String(50))
    friday = Column(String(50))
    saturday = Column(String(50))
    sunday = Column(String(50))
    start_date = Column(String(50))
    end_date = Column(String(50))

    def __repr__(self):
        return "<Calendar(service_id='%s', start_date='%s', end_date='%s')>"\
            % (self.service_id, self.start_date, self.end_date)

    def __str__(self):
            return self.__repr__()


class CalendarDate(RdbModel):
    __tablename__ = 'calendar_dates'

    service_id = Column(String(50), primary_key=True)
    date = Column(String(50), primary_key=True)
    exception_type = Column(String(50), primary_key=True)

    def __repr__(self):
        return "<CalendarDate(service_id='%s', date='%s', exception_type='%s')>"\
            % (self.service_id,
               self.date,
               "added (1)" if int(self.exception_type) == 1 else "removed (2)"
               )

    def __str__(self):
        return self.__repr__()
