"""
Module used to query Transilien's API.
"""

from os import path
import logging
import asyncio
from aiohttp import ClientSession
from datetime import datetime, timedelta
import requests
import time

from api_etl.utils_secrets import get_secret

BASE_DIR = path.dirname(
    path.dirname(path.abspath(__file__)))


API_USER = get_secret("API_USER")
API_PASSWORD = get_secret("API_PASSWORD")

_RETRIABLE_STATUSES = set([500, 503, 504])


class ApiClient():
    """
    This class provide a client to process requests to transilien's API.
    It provides methods to process either single queries, or asynchronous batch queries that rely on asyncio library.
    """

    def __init__(self, user=API_USER, password=API_PASSWORD, retry_timeout=20, core_url='http://api.transilien.com/'):
        self.core_url = core_url
        self.user = user
        self.password = password
        self.retry_timeout = retry_timeout
        self.requested_urls = []

    def _get(self, url, extra_params=None, verbose=False, first_request_time=None, retry_counter=0):
        """
        Low level function to process single queries with retries.
        """
        if verbose and not first_request_time:
            logging.debug("Import on url %s ", url)

        if not first_request_time:
            first_request_time = datetime.now()

        elapsed = datetime.now() - first_request_time
        if elapsed > timedelta(seconds=self.retry_timeout):
            raise TimeoutError

        if retry_counter > 0:
            # 0.5 * (1.5 ^ i) is an increased sleep time of 1.5x per iteration,
            # starting at 0.5s when retry_counter=0. The first retry will occur
            # at 1, so subtract that first.
            delay_seconds = 0.5 * 1.5 ** (retry_counter - 1)
            time.sleep(delay_seconds)

        full_url = path.join(self.core_url, url)

        response = requests.get(
            url=full_url, auth=(self.user, self.password), params=(extra_params or {}))
        self.requested_urls.append(response.url)

        # Warn if not 200
        if response.status_code != 200:
            logging.debug("WARNING: response status_code is %s",
                          response.status_code)

        if response.status_code in _RETRIABLE_STATUSES:
            # Retry request.
            logging.debug("WARNING: retry number %d", retry_counter)
            return self._get(url=url, extra_params=extra_params, first_request_time=first_request_time, retry_counter=retry_counter + 1, verbose=verbose)

        return response

    def request_station(self, station, verbose=False, extra_params=None):
        """
        This method process a single query.

        :param station: station you want to query (8 digits format)
        :type station: str/int (8 digits format)

        :rtype: str (xml answer)
        """
        # example_url = "http://api.transilien.com/gare/87393009/depart/"
        url = path.join("gare", str(station), "depart")
        return self._get(url=url, verbose=verbose, extra_params=extra_params)

    def _stations_to_full_urls(self, station_list):
        """
        Hack function for asynchronous calls.
        """
        full_url_list = []
        for station in station_list:
            full_url = path.join(
                self.core_url, "gare", str(station), "depart")
            # remove http:// from full_url and add it at beginning
            full_url = "http://%s:%s@%s" % (self.user,
                                            self.password, full_url[7:])
            full_url_list.append(full_url)
        return full_url_list

    def request_stations(self, station_list):
        """
        This method process asynchronous batch queries. It will return answers with station ids so that you can identify stations answers.

        :param station_list: list of station_ids in the 8 digits format used by transilien's API to identify stations (warning: different than station ids in GTFS files that are 7 digits).
        :type station_list: list of str

        :rtype: list of tuples (api_response, station_id)
        """
        def url_to_station(url):
            station = url.split("/")[-2]
            return station

        full_urls = self._stations_to_full_urls(station_list)

        async def fetch(url, session):
            async with session.get(url) as response:
                try:
                    resp = await response.read()
                    station = url_to_station(url)
                    return [resp, station]
                except:
                    logging.debug(
                        "Error getting station %s information", station)
                    return [False, station]

        async def run(url_list):
            tasks = []
            # Fetch all responses within one Client session,
            # keep connection alive for all requests.
            async with ClientSession() as session:
                for url in url_list:
                    task = asyncio.ensure_future(
                        fetch(url, session))
                    tasks.append(task)

                responses = await asyncio.gather(*tasks)
                # you now have all response bodies in this variable
                # print(responses)
                return responses

        # def print_responses(result):
        #    print(result)
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(full_urls))
        loop.run_until_complete(future)
        return future.result()
