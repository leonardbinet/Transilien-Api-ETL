
from datetime import datetime, timedelta
import requests
import os
import time

_RETRIABLE_STATUSES = set([500, 503, 504])


class ApiClient():

    def __init__(self, user, password="", retry_timeout=20, core_url='https://api.navitia.io/v1/'):
        self.core_url = core_url
        self.user = user
        self.password = password
        self.retry_timeout = retry_timeout
        self.requested_urls = []

    def _get(self, url, extra_params=None, verbose=False, first_request_time=None, retry_counter=0, ignore_fail=False):
        if verbose and not first_request_time:
            print("Import on url %s " % url)

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

        try:
            response = requests.get(
                url=full_url, auth=(self.user, self.password), params=(extra_params or {}))
            self.requested_urls.append(response.url)

        except Exception as e:
            if not ignore_fail:
                raise SystemError
            else:
                return False

        # Warn if not 200
        if response.status_code != 200:
            print("WARNING: response status_code is %s" % response.status_code)

        if response.status_code in _RETRIABLE_STATUSES:
            # Retry request.
            print("WARNING: retry number %d" % retry_counter)
            return self._get(url=url, extra_params=extra_params, first_request_time=first_request_time, retry_counter=retry_counter + 1, verbose=verbose, ignore_fail=ignore_fail)

        return response
