""" Data Models for databases: relational databases, and Dynamo.
"""
import logging

from pynamodb.models import Model as DyModel
from pynamodb.attributes import UnicodeAttribute

from sqlalchemy.ext import declarative
from sqlalchemy import Column, String, ForeignKey

from mongoengine import DynamicDocument, StringField

from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter
from api_etl.utils_secrets import get_secret
from api_etl.settings import dynamo_realtime, dynamo_schedule

# Set as environment variable: boto takes it directly
AWS_DEFAULT_REGION = get_secret("AWS_DEFAULT_REGION", env=True)
AWS_ACCESS_KEY_ID = get_secret("AWS_ACCESS_KEY_ID", env=True)
AWS_SECRET_ACCESS_KEY = get_secret("AWS_SECRET_ACCESS_KEY", env=True)

RdbModel = declarative.declarative_base()


class RealTimeDeparture(DyModel):

    class Meta:
        table_name = dynamo_realtime["name"]
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

    def has_passed(self, at_datetime=None, seconds=False):
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

        return (time_past_dep >= 0)


class ScheduledDeparture(DyModel):

    class Meta:
        table_name = dynamo_schedule["name"]
        region = AWS_DEFAULT_REGION

    # Fields added for indexing and identification
    day_train_num = UnicodeAttribute(range_key=True)
    station_id = UnicodeAttribute(hash_key=True)
    # Train num and trip

    # Necessary attributes
    trip_id = UnicodeAttribute()
    train_num = UnicodeAttribute()
    stop_sequence = UnicodeAttribute()
    # These dates are in 'weird' format -> 27h
    scheduled_departure_time = UnicodeAttribute()
    scheduled_departure_day = UnicodeAttribute()

    route_id = UnicodeAttribute()
    service_id = UnicodeAttribute()

    # Optional attributes
    pickup_type = UnicodeAttribute(null=True)
    stop_id = UnicodeAttribute(null=True)
    arrival_time = UnicodeAttribute(null=True)
    route_short_name = UnicodeAttribute(null=True)
    stop_headsign = UnicodeAttribute(null=True)
    block_id = UnicodeAttribute(null=True)
    drop_off_type = UnicodeAttribute(null=True)
    trip_headsign = UnicodeAttribute(null=True)
    direction_id = UnicodeAttribute(null=True)


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


class Trip(RdbModel):
    __tablename__ = 'trips'

    trip_id = Column(String(50), primary_key=True)
    route_id = Column(String(50), ForeignKey('routes.route_id'))
    service_id = Column(String(50))
    trip_headsign = Column(String(50))
    direction_id = Column(String(50))
    block_id = Column(String(50))


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

    def get_partial_index(self):
        self._station_id = self.stop_id[-7:]
        self._train_num = self.trip_id[5:11]
        return (self._station_id, self._train_num)

    def get_realtime_index(self, yyyymmdd):
        self.get_partial_index()
        self._yyyymmdd = yyyymmdd
        self._day_train_num = "%s_%s" % (yyyymmdd, self._train_num)
        return (self._station_id, self._day_train_num)

    def has_passed(self, at_datetime=None, seconds=False):
        """ Checks if train expected passage time has passed, based on:
        - expected_passage_time we got from realtime api.
        - scheduled_departure_time from gtfs
        And sets it as attributes.
        """
        if not at_datetime:
            at_datetime = get_paris_local_datetime_now().replace(tzinfo=None)

        dt = self.departure_time
        dd = self._yyyymmdd

        time_past_dep = DateConverter(dt=at_datetime)\
            .compute_delay_from(special_date=dd, special_time=dt)

        if seconds:
            # return number of seconds instead of boolean
            return time_past_dep

        return (time_past_dep >= 0)


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


class CalendarDate(RdbModel):
    __tablename__ = 'calendar_dates'

    service_id = Column(String(50), primary_key=True)
    date = Column(String(50), primary_key=True)
    exception_type = Column(String(50), primary_key=True)


class FlatStopTime(DynamicDocument):
    # Delay = StringField(max_length=200, required=True)
    # Passed = StringField(max_length=200, required=True)

    required_attributes = [
        'Agency_agency_id', 'Agency_agency_lang', 'Agency_agency_name',
        'Agency_agency_timezone', 'Agency_agency_url', 'Calendar_end_date',
        'Calendar_friday', 'Calendar_monday', 'Calendar_saturday',
        'Calendar_service_id', 'Calendar_start_date', 'Calendar_sunday',
        'Calendar_thursday', 'Calendar_tuesday', 'Calendar_wednesday',
        'RealTime_data_freshness', 'RealTime_date',
        'RealTime_day_train_num', 'RealTime_etat',
        'RealTime_expected_passage_day', 'RealTime_expected_passage_time',
        'RealTime_miss', 'RealTime_request_day', 'RealTime_request_time',
        'RealTime_station_8d', 'RealTime_station_id', 'RealTime_term',
        'RealTime_train_num', 'Route_agency_id', 'Route_route_color',
        'Route_route_desc', 'Route_route_id', 'Route_route_long_name',
        'Route_route_short_name', 'Route_route_text_color',
        'Route_route_type', 'Route_route_url', 'StopTime_arrival_time',
        'StopTime_day_train_num',
        'StopTime_departure_time', 'StopTime_drop_off_type',
        'StopTime_pickup_type',
        'StopTime_station_id',
        'StopTime_stop_headsign', 'StopTime_stop_id',
        'StopTime_stop_sequence', 'StopTime_train_num',
        'StopTime_yyyymmdd', 'Stop_location_type', 'Stop_parent_station',
        'Stop_stop_desc', 'Stop_stop_lat', 'Stop_stop_lon',
        'Stop_stop_name', 'Stop_stop_url', 'Stop_zone_id', 'Trip_block_id',
        'Trip_direction_id', 'Trip_route_id', 'Trip_service_id',
        'Trip_trip_headsign', 'Trip_trip_id'
    ]
    for attribute in required_attributes:
        # setattr(FlatStopTime, attribute, StringField(
        #    max_length=200, required=True))
        vars()[attribute] = StringField(
            max_length=200, required=True
        )
    Delay = StringField(max_length=200, required=True)
    Passed = StringField(max_length=200, required=True)

    StopTime_trip_id = StringField(max_length=200, required=True)
    Stop_stop_id = StringField(
        max_length=200, required=True,
        unique_with='StopTime_trip_id'
    )
