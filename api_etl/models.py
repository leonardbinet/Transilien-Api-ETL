""" Data Models for relational databases
"""
import logging

from sqlalchemy.ext import declarative
from sqlalchemy import Column, String, ForeignKey
from pynamodb.exceptions import DoesNotExist
# from sqlalchemy.orm import relationship

from api_etl.utils_dynamo import RealTimeDeparture
from api_etl.utils_misc import get_paris_local_datetime_now, DateConverter

logger = logging.getLogger(__name__)

Model = declarative.declarative_base()


class Agency(Model):
    __tablename__ = 'agencies'

    agency_id = Column(String(50), primary_key=True)
    agency_name = Column(String(50))
    agency_url = Column(String(50))
    agency_timezone = Column(String(50))
    agency_lang = Column(String(50))

    def __repr__(self):
        return "<Agency(agency_id='%s', agency_name='%s', agency_url='%s')>"\
            % (self.agency_id, self.agency_name, self.agency_url)


class Route(Model):
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


class Trip(Model):
    __tablename__ = 'trips'

    trip_id = Column(String(50), primary_key=True)
    route_id = Column(String(50), ForeignKey('routes.route_id'))
    service_id = Column(String(50))
    trip_headsign = Column(String(50))
    direction_id = Column(String(50))
    block_id = Column(String(50))


class StopTime(Model):
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
        self.station_id = self.stop_id[-7:]
        self.train_num = self.trip_id[5:11]
        return (self.station_id, self.train_num)

    def get_realtime_index(self, yyyymmdd):
        self.get_partial_index()
        self.yyyymmdd = yyyymmdd
        self.day_train_num = "%s_%s" % (yyyymmdd, self.train_num)
        return (self.station_id, self.day_train_num)

    def set_realtime(self, realtime_object=None):
        assert isinstance(realtime_object, RealTimeDeparture)
        if realtime_object:
            self._realtime_object = realtime_object
            self._realtime_dict = self._realtime_object.attribute_values
            self.realtime_found = True
            self._compute_delay()
            self._has_passed()
        else:
            self.realtime_found = False

    def get_realtime_info(self, yyyymmdd, ignore_error=True):
        self.get_realtime_index(yyyymmdd)

        # Try to get it from dynamo
        try:
            realtime_object = RealTimeDeparture.get(
                hash_key=self.station_id,
                range_key=self.day_train_num
            )
            self.set_realtime(realtime_object=realtime_object)

        except DoesNotExist:
            self.set_realtime(realtime_object=False)
            logger.info("Realtime not found for %s, %s" %
                        (self.station_id, self.day_train_num))
            if not ignore_error:
                raise DoesNotExist

    def _compute_delay(self):
        """ Between scheduled 'stop time' departure time, and realtime expected
        departure time.
        """
        sdt = self.departure_time
        sdd = self.yyyymmdd
        rtdt = self._realtime_object.expected_passage_time
        rtdd = self._realtime_object.expected_passage_day
        self.delay = DateConverter(normal_date=rtdd, normal_time=rtdt)\
            .compute_delay_from(special_date=sdd, special_time=sdt)
        return self.delay

    def _has_passed(self):
        """ Checks if train expected passage time has passed
        """
        rtdt = self._realtime_object.expected_passage_time
        rtdd = self._realtime_object.expected_passage_day
        cdt = get_paris_local_datetime_now().replace(tzinfo=None)
        timepastdep = DateConverter(dt=cdt)\
            .compute_delay_from(normal_date=rtdd, normal_time=rtdt)
        self.passed = (timepastdep >= 0)


class Stop(Model):
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


class Calendar(Model):
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


class CalendarDate(Model):
    __tablename__ = 'calendar_dates'

    service_id = Column(String(50), primary_key=True)
    date = Column(String(50), primary_key=True)
    exception_type = Column(String(50), primary_key=True)
