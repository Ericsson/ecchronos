#
# Copyright 2019 Telefonaktiebolaget LM Ericsson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

try:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError, URLError
except ImportError:
    from urllib2 import urlopen, Request, HTTPError, URLError
import json
from ecchronoslib.types import RepairJob, VerboseRepairJob


class RequestResult:
    def __init__(self, status_code=None, data=None, exception=None, message=None):
        self.status_code = status_code
        self.data = data
        self.exception = exception
        self.message = message

    def format_exception(self):
        msg = "Encountered issue"

        if self.status_code is not None:
            msg = "{0} ({1})".format(msg, self.status_code)

        if self.message is not None:
            msg = "{0} '{1}'".format(msg, self.message)

        if self.exception is not None:
            msg = "{0}: {1}".format(msg, self.exception)

        return msg

    def is_successful(self):
        return self.status_code == 200

    def transform_with_data(self, new_data):
        return RequestResult(status_code=self.status_code, data=new_data, exception=self.exception, message=self.message)


class RestRequest:
    default_base_url = 'http://localhost:8080'

    def __init__(self, base_url=None):
        self.base_url = base_url if base_url is not None else RestRequest.default_base_url

    @staticmethod
    def get_param(httpmessage, param):
        try:
            return httpmessage.get_param(param)
        except AttributeError:
            return httpmessage.getparam(param)

    @staticmethod
    def get_charset(response):
        return RestRequest.get_param(response.info(), 'charset') or 'utf-8'

    def request(self, url):
        request_url = "{0}/{1}".format(self.base_url, url)
        try:
            request = Request(request_url)
            response = urlopen(request)
            json_data = json.loads(response.read().decode(RestRequest.get_charset(response)))

            response.close()
            return RequestResult(status_code=200, data=json_data)
        except HTTPError as e:
            return RequestResult(status_code=e.code, exception=e)
        except URLError as e:
            return RequestResult(status_code=404, message="Unable to connect to {0}".format(request_url), exception=e)
        except Exception as e:
            return RequestResult(exception=e, message="Unable to retrieve resource {0}".format(request_url))


class RepairSchedulerRequest(RestRequest):
    repair_scheduler_list_url = 'repair-scheduler/v1/list'
    repair_scheduler_get_url = 'repair-scheduler/v1/get/{0}/{1}'

    def __init__(self, base_url=None):
        RestRequest.__init__(self, base_url)

    def get(self, keyspace, table):
        request_url = RepairSchedulerRequest.repair_scheduler_get_url.format(keyspace, table)

        result = self.request(request_url)

        if result.is_successful():
            result = result.transform_with_data(new_data=VerboseRepairJob(result.data))

        return result

    def list(self, keyspace=None):
        request_url = RepairSchedulerRequest.repair_scheduler_list_url
        if keyspace is not None:
            request_url = "{0}/{1}".format(request_url, keyspace)

        result = self.request(request_url)

        if result.is_successful():
            result = result.transform_with_data(new_data=[RepairJob(x) for x in result.data])

        return result

