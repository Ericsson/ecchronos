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
    from urllib.parse import quote
except ImportError:
    from urllib2 import urlopen, Request, HTTPError, URLError
    from urllib import quote # pylint: disable=ungrouped-imports
import json
import os
import ssl
from ecchronoslib.types import FullSchedule, Repair, Schedule, RepairInfo


class RequestResult(object):
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
        return RequestResult(status_code=self.status_code,
                             data=new_data,
                             exception=self.exception,
                             message=self.message)


class RestRequest(object):
    default_base_url = 'http://localhost:8080'
    default_https_base_url = 'https://localhost:8080'

    def __init__(self, base_url=None):
        if base_url:
            self.base_url = base_url
        elif os.getenv("ECCTOOL_CERT_FILE") and os.getenv("ECCTOOL_KEY_FILE") and os.getenv("ECCTOOL_CA_FILE"):
            self.base_url = RestRequest.default_https_base_url
        else:
            self.base_url = RestRequest.default_base_url

    @staticmethod
    def get_param(httpmessage, param):
        try:
            return httpmessage.get_param(param)
        except AttributeError:
            return httpmessage.getparam(param)

    @staticmethod
    def get_charset(response):
        return RestRequest.get_param(response.info(), 'charset') or 'utf-8'

    def request(self, url, method='GET'):
        request_url = "{0}/{1}".format(self.base_url, url)
        try:
            request = Request(request_url)
            request.get_method = lambda: method
            cert_file = os.getenv("ECCTOOL_CERT_FILE")
            key_file = os.getenv("ECCTOOL_KEY_FILE")
            ca_file = os.getenv("ECCTOOL_CA_FILE")
            if cert_file and key_file and ca_file:
                context = ssl.create_default_context(cafile=ca_file)
                context.load_cert_chain(cert_file, key_file)
                response = urlopen(request, context=context)
            else:
                response = urlopen(request)
            json_data = json.loads(response.read().decode(RestRequest.get_charset(response)))

            response.close()
            return RequestResult(status_code=response.getcode(), data=json_data)
        except HTTPError as e:
            return RequestResult(status_code=e.code,
                                 message="Unable to retrieve resource {0}".format(request_url),
                                 exception=e)
        except URLError as e:
            return RequestResult(status_code=404,
                                 message="Unable to connect to {0}".format(request_url),
                                 exception=e)
        except Exception as e:  # pylint: disable=broad-except
            return RequestResult(exception=e,
                                 message="Unable to retrieve resource {0}".format(request_url))


class V2RepairSchedulerRequest(RestRequest):
    ROOT = 'repair-management/'
    PROTOCOL = ROOT + 'v2/'
    REPAIRS = PROTOCOL + 'repairs'
    SCHEDULES = PROTOCOL + 'schedules'

    v2_schedule_status_url = SCHEDULES
    v2_schedule_id_status_url = SCHEDULES + '/{0}'
    v2_schedule_id_full_status_url = SCHEDULES + '/{0}?full=true'

    v2_repair_status_url = REPAIRS
    v2_repair_id_status_url = REPAIRS + '/{0}'

    v2_repair_run_url = REPAIRS

    repair_info_url = PROTOCOL + 'repairInfo'

    def __init__(self, base_url=None):
        RestRequest.__init__(self, base_url)

    def get_schedule(self, job_id, full=False):
        if full:
            request_url = V2RepairSchedulerRequest.v2_schedule_id_full_status_url.format(job_id)
        else:
            request_url = V2RepairSchedulerRequest.v2_schedule_id_status_url.format(job_id)

        result = self.request(request_url)
        if result.is_successful():
            result = result.transform_with_data(new_data=FullSchedule(result.data))

        return result

    def get_repair(self, job_id, host_id=None):
        request_url = V2RepairSchedulerRequest.v2_repair_id_status_url.format(job_id)
        if host_id:
            request_url += "?hostId={0}".format(host_id)
        result = self.request(request_url)
        if result.is_successful():
            result = result.transform_with_data(new_data=[Repair(x) for x in result.data])

        return result

    def list_schedules(self, keyspace=None, table=None):
        request_url = V2RepairSchedulerRequest.v2_schedule_status_url

        if keyspace and table:
            request_url = "{0}?keyspace={1}&table={2}".format(request_url, keyspace, table)
        elif keyspace:
            request_url = "{0}?keyspace={1}".format(request_url, keyspace)

        result = self.request(request_url)

        if result.is_successful():
            result = result.transform_with_data(new_data=[Schedule(x) for x in result.data])

        return result

    def list_repairs(self, keyspace=None, table=None, host_id=None):
        request_url = V2RepairSchedulerRequest.v2_repair_status_url
        if keyspace:
            request_url = "{0}?keyspace={1}".format(request_url, keyspace)
            if table:
                request_url += "&table={0}".format(table)
            if host_id:
                request_url += "&hostId={0}".format(host_id)
        elif host_id:
            request_url += "?hostId={0}".format(host_id)

        result = self.request(request_url)

        if result.is_successful():
            result = result.transform_with_data(new_data=[Repair(x) for x in result.data])

        return result

    def post(self, keyspace=None, table=None, local=False, repair_type="vnode"):
        request_url = V2RepairSchedulerRequest.v2_repair_run_url
        if keyspace:
            request_url += "?keyspace=" + keyspace
            if table:
                request_url += "&table=" + table
        if local:
            if keyspace:
                request_url += "&isLocal=true"
            else:
                request_url += "?isLocal=true"
        if repair_type:
            if keyspace or local:
                request_url += "&repairType=" + repair_type
            else:
                request_url += "?repairType=" + repair_type
        result = self.request(request_url, 'POST')
        if result.is_successful():
            result = result.transform_with_data(new_data=[Repair(x) for x in result.data])
        return result

    def get_repair_info(self, keyspace=None, table=None, since=None,  # pylint: disable=too-many-arguments
                        duration=None, local=False):
        request_url = V2RepairSchedulerRequest.repair_info_url
        if keyspace:
            request_url += "?keyspace=" + quote(keyspace)
            if table:
                request_url += "&table=" + quote(table)
        if local:
            if keyspace:
                request_url += "&isLocal=true"
            else:
                request_url += "?isLocal=true"
        if since:
            if keyspace or local:
                request_url += "&since=" + quote(since)
            else:
                request_url += "?since=" + quote(since)
        if duration:
            if keyspace or since or local:
                request_url += "&duration=" + quote(duration)
            else:
                request_url += "?duration=" + quote(duration)
        result = self.request(request_url)
        if result.is_successful():
            result = result.transform_with_data(new_data=RepairInfo(result.data))
        return result
