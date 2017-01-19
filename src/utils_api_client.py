import os
from os import path, sys
import logging
import asyncio
from aiohttp import ClientSession
from datetime import datetime, timedelta
import requests
import time

if __name__ == '__main__':
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from src.utils_secrets import get_secret


BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))


API_USER = get_secret("API_USER")
API_PASSWORD = get_secret("API_PASSWORD")

_RETRIABLE_STATUSES = set([500, 503, 504])


def get_api_client():
    return ApiClient(user=API_USER, password=API_PASSWORD)


class ApiClient():

    def __init__(self, user, password="", retry_timeout=20, core_url='http://api.transilien.com/'):
        self.core_url = core_url
        self.user = user
        self.password = password
        self.retry_timeout = retry_timeout
        self.requested_urls = []

    def _get(self, url, extra_params=None, verbose=False, first_request_time=None, retry_counter=0):
        if verbose and not first_request_time:
            logging.debug("Import on url %s " % url)

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

        full_url = os.path.join(self.core_url, url)

        response = requests.get(
            url=full_url, auth=(self.user, self.password), params=(extra_params or {}))
        self.requested_urls.append(response.url)

        # Warn if not 200
        if response.status_code != 200:
            logging.debug("WARNING: response status_code is %s" %
                          response.status_code)

        if response.status_code in _RETRIABLE_STATUSES:
            # Retry request.
            logging.debug("WARNING: retry number %d" % retry_counter)
            return self._get(url=url, extra_params=extra_params, first_request_time=first_request_time, retry_counter=retry_counter + 1, verbose=verbose)

        return response

    def request_station(self, station, verbose=False, ignore_fail=False, extra_params=None):
        # example_url = "http://api.transilien.com/gare/87393009/depart/"
        url = os.path.join("gare", str(station), "depart")
        return self._get(url=url, verbose=verbose, extra_params=extra_params)

    def _stations_to_full_urls(self, station_list):
        full_url_list = []
        for station in station_list:
            full_url = os.path.join(
                self.core_url, "gare", str(station), "depart")
            # remove http:// from full_url and add it at beginning
            full_url = "http://%s:%s@%s" % (self.user,
                                            self.password, full_url[7:])
            full_url_list.append(full_url)
        return full_url_list

    def request_stations(self, station_list):
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
                        "Error getting station %s information" % station)
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


if __name__ == '__main__':
    # https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
    # asyncio tutorial

    # operate_timer()
    import pandas as pd

    data_path = os.path.join(BASE_DIR, "data")
    df_gares = pd.read_csv(os.path.join(
        data_path, "gares_transilien.csv"), sep=";")
    station_ids = df_gares["Code UIC"].values

    client = get_api_client()
    responses = client.request_stations(station_ids)
