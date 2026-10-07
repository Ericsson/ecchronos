#
# Copyright 2025 Telefonaktiebolaget LM Ericsson
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
    from urllib3 import urlopen, Request, HTTPError, URLError
    from urllib import quote  # pylint: disable=ungrouped-imports
import json
import os
import socket
import ssl
from ecchronoslib.types import FullSchedule, Repair, Schedule, RepairInfo, NodeSyncState, Rejection, RepairSession

DEFAULT_TIMEOUT_SECONDS = 30.0
TIMEOUT_ENV_VAR = "ECCTOOL_TIMEOUT_SECONDS"


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
        return RequestResult(
            status_code=self.status_code, data=new_data, exception=self.exception, message=self.message
        )


class RestRequest(object):
    default_base_url = "http://localhost:8080"
    default_https_base_url = "https://localhost:8080"

    def __init__(self, base_url=None, timeout=None):
        if base_url:
            self.base_url = base_url
        elif os.getenv("ECCTOOL_CERT_FILE") and os.getenv("ECCTOOL_KEY_FILE") and os.getenv("ECCTOOL_CA_FILE"):
            self.base_url = RestRequest.default_https_base_url
        else:
            self.base_url = RestRequest.default_base_url
        self.timeout = _resolve_timeout(timeout)

    @staticmethod
    def get_param(httpmessage, param):
        try:
            return httpmessage.get_param(param)
        except AttributeError:
            return httpmessage.getparam(param)

    @staticmethod
    def get_charset(response):
        return RestRequest.get_param(response.info(), "charset") or "utf-8"

    def _urlopen(self, request):
        """Open a request applying the configured timeout and TLS context.

        Centralizes timeout and client-certificate handling so every call
        site (request/basic_request) behaves consistently.
        """
        context = _create_ssl_context()
        if context is not None:
            return urlopen(request, context=context, timeout=self.timeout)
        return urlopen(request, timeout=self.timeout)

    def _connection_error(self, request_url, exc):
        """Build a RequestResult for a connection failure, clarifying timeouts.

        Handles both a connect-phase timeout (wrapped as URLError with a
        socket.timeout reason) and a read-phase timeout (a bare
        socket.timeout raised while reading the response).
        """
        if isinstance(exc, socket.timeout) or isinstance(getattr(exc, "reason", None), socket.timeout):
            message = "Request to {0} timed out after {1}s".format(request_url, self.timeout)
        else:
            message = "Unable to connect to {0}".format(request_url)
        return RequestResult(status_code=404, message=message, exception=exc)

    def request(self, url, method="GET", body=None, headers=None):
        request_url = "{0}/{1}".format(self.base_url, url)
        try:
            if body is not None:
                if isinstance(body, dict):
                    body = json.dumps(body)
                body = body.encode("utf-8")
            request = Request(request_url, data=body)

            if headers:
                for k, v in headers.items():
                    request.add_header(k, v)
            request.get_method = lambda: method
            response = self._urlopen(request)
            json_data = json.loads(response.read().decode(RestRequest.get_charset(response)))

            response.close()
            return RequestResult(status_code=response.getcode(), data=json_data)
        except HTTPError as e:
            error_body = None
            try:
                error_body = e.read().decode("utf-8")
                error_json = json.loads(error_body)
                error_msg = error_json.get("message", error_body)
            except (ValueError, UnicodeDecodeError, OSError):
                error_msg = error_body
            return RequestResult(
                status_code=e.code,
                message=error_msg or "Unable to retrieve resource {0}".format(request_url),
                exception=e,
            )
        except URLError as e:
            return self._connection_error(request_url, e)
        except socket.timeout as e:
            return self._connection_error(request_url, e)
        except Exception as e:  # pylint: disable=broad-except
            return RequestResult(exception=e, message="Unable to retrieve resource {0}".format(request_url))

    def basic_request(self, url, method="GET", headers=None):
        request_url = "{0}/{1}".format(self.base_url, url)
        try:
            request = Request(request_url)
            request.get_method = lambda: method
            if headers:
                for k, v in headers.items():
                    request.add_header(k, v)
            response = self._urlopen(request)

            data = response.read()

            response.close()
            return data.decode("UTF-8")
        except HTTPError as e:
            return RequestResult(
                status_code=e.code, message="Unable to retrieve resource {0}".format(request_url), exception=e
            )
        except URLError as e:
            return self._connection_error(request_url, e)
        except socket.timeout as e:
            return self._connection_error(request_url, e)
        except Exception as e:  # pylint: disable=broad-except
            return RequestResult(exception=e, message="Unable to retrieve resource {0}".format(request_url))


class RepairSchedulerRequest(RestRequest):
    ROOT = "repair-management/"
    REPAIRS = ROOT + "repairs"
    SCHEDULES = ROOT + "schedules"

    schedule_status_url = SCHEDULES
    schedule_id_status_url = SCHEDULES + "/{0}"
    schedule_id_job_status_url = SCHEDULES + "/{0}/{1}"
    keyspace_and_table_url = "?keyspace={0}&table={1}"
    keyspace_url = "?keyspace={0}"

    repair_status_url = REPAIRS
    repair_id_status_url = REPAIRS + "/{0}"

    repair_run_url = REPAIRS

    repair_info_url = ROOT + "repairInfo/{0}"

    running_job_url = ROOT + "running-job"

    def __init__(self, base_url=None, timeout=None):
        RestRequest.__init__(self, base_url, timeout=timeout)

    def get_schedule(
        self, node_id, keyspace, table, job_id=None, full=False
    ):  # pylint: disable=too-many-arguments, too-many-positional-arguments
        if job_id is not None:
            request_url = RepairSchedulerRequest.schedule_id_job_status_url.format(node_id, job_id)
        else:
            request_url = RepairSchedulerRequest.schedule_id_status_url.format(node_id)
        if keyspace is not None and table is not None:
            request_url = request_url + RepairSchedulerRequest.keyspace_and_table_url.format(keyspace, table)

        if keyspace is not None and table is None:
            request_url = request_url + RepairSchedulerRequest.keyspace_url.format(keyspace)
        if full and keyspace is not None:
            request_url = request_url + "&full=true"
        if full and keyspace is None:
            request_url = request_url + "?full=true"

        result = self.request(request_url)
        if result.is_successful():
            if isinstance(result.data, list):
                result = result.transform_with_data(new_data=[Schedule(x) for x in result.data])
            else:
                result = result.transform_with_data(new_data=FullSchedule(result.data))
        return result

    def get_repair(self, node_id, job_id):
        request_url = RepairSchedulerRequest.repair_id_status_url.format(node_id)
        if job_id:
            request_url += "?jobID={0}".format(job_id)
        result = self.request(request_url)
        if result.is_successful():
            result = result.transform_with_data(new_data=[Repair(x) for x in result.data])

        return result

    def list_schedules(self, keyspace=None, table=None):
        request_url = RepairSchedulerRequest.schedule_status_url

        if keyspace and table:
            request_url = "{0}?keyspace={1}&table={2}".format(request_url, keyspace, table)
        elif keyspace:
            request_url = "{0}?keyspace={1}".format(request_url, keyspace)

        result = self.request(request_url)

        if result.is_successful():
            result = result.transform_with_data(new_data=[Schedule(x) for x in result.data])

        return result

    def list_repairs(self, keyspace=None, table=None, host_id=None):
        request_url = RepairSchedulerRequest.repair_status_url
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

    def post(
        self,
        node_id=None,
        keyspace=None,
        table=None,
        repair_type="vnode",
        allnodes="false",
        force_repair_twcs="false",
        force_repair_disabled="false",
    ):  # pylint: disable=too-many-arguments, too-many-positional-arguments
        request_url = RepairSchedulerRequest.repair_run_url
        separator = "?"
        if node_id:
            request_url += separator + "nodeID=" + node_id
            separator = "&"
        if keyspace:
            request_url += separator + "keyspace=" + keyspace
            separator = "&"
            if table:
                request_url += "&table=" + table
        if repair_type:
            request_url += separator + "repairType=" + repair_type
            separator = "&"
        if allnodes is True:
            request_url += separator + "all=true"
            separator = "&"
        if force_repair_twcs is True:
            request_url += separator + "forceRepairTWCS=true"
            separator = "&"
        if force_repair_disabled is True:
            request_url += separator + "forceRepairDisabled=true"
            separator = "&"
        result = self.request(request_url, "POST")
        if result.is_successful():
            result = result.transform_with_data(new_data=[Repair(x) for x in result.data])
        return result

    def get_repair_info(
        self, node_id=None, keyspace=None, table=None, since=None, duration=None, local=False
    ):  # pylint: disable=too-many-arguments, too-many-positional-arguments
        request_url = RepairSchedulerRequest.repair_info_url.format(node_id)
        if keyspace:
            request_url += "?keyspace=" + quote(keyspace)
            if table:
                request_url += "&table=" + quote(table)
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

    def running_job(self):
        request_url = RepairSchedulerRequest.running_job_url
        result = self.basic_request(request_url)
        return result


class StateManagementRequest(RestRequest):
    ROOT = "state/"
    NODES = ROOT + "nodes"

    def __init__(self, base_url=None, timeout=None):
        RestRequest.__init__(self, base_url, timeout=timeout)

    def get_nodes(self):
        result = self.request(StateManagementRequest.NODES)
        if result.is_successful():
            result = result.transform_with_data(new_data=[NodeSyncState(x) for x in result.data])
        return result


class RejectionsRequest(RestRequest):
    ROOT = "rejections"
    TRUNCATE = ROOT + "/all"

    def __init__(self, base_url=None, timeout=None):
        RestRequest.__init__(self, base_url, timeout=timeout)

    def list_rejections(self, keyspace=None, table=None):
        request_url = RejectionsRequest.ROOT

        if keyspace and table:
            request_url = "{0}?keyspace={1}&table={2}".format(request_url, keyspace, table)
        elif keyspace:
            request_url = "{0}?keyspace={1}".format(request_url, keyspace)

        result = self.request(request_url)

        if result.is_successful():
            result = result.transform_with_data(new_data=[Rejection(x) for x in result.data])

        return result

    def create_rejection(self, rejection_body):
        request_url = RejectionsRequest.ROOT
        headers = {"Content-Type": "application/json"}
        result = self.request(request_url, "POST", body=rejection_body, headers=headers)

        if result.is_successful():
            new_result = result.transform_with_data(new_data=[Rejection(x) for x in result.data["data"]])
            new_result.message = result.data["message"]
            result = new_result
        return result

    def delete_rejection(self, rejection_body):
        request_url = RejectionsRequest.ROOT
        headers = {"Content-Type": "application/json"}
        result = self.request(request_url, "DELETE", body=rejection_body, headers=headers)

        if result.is_successful():
            new_result = result.transform_with_data(new_data=[Rejection(x) for x in result.data["data"]])
            new_result.message = result.data["message"]
            result = new_result
        return result

    def truncate_rejections(self):
        request_url = RejectionsRequest.TRUNCATE
        result = self.request(request_url, "DELETE")

        if result.is_successful():
            new_result = result.transform_with_data(new_data=[Rejection(x) for x in result.data["data"]])
            new_result.message = result.data["message"]
            result = new_result
        return result

    def update_rejection(self, rejection_body):
        request_url = RejectionsRequest.ROOT
        headers = {"Content-Type": "application/json"}
        result = self.request(request_url, "PATCH", body=rejection_body, headers=headers)
        if result.is_successful():
            new_result = result.transform_with_data(new_data=[Rejection(x) for x in result.data["data"]])
            new_result.message = result.data["message"]
            result = new_result
        return result


class ConfigRequest(RestRequest):
    URL = "repair-management/v2/config"

    def __init__(self, base_url=None, timeout=None):
        RestRequest.__init__(self, base_url, timeout=timeout)

    def get(self):
        return self.request(ConfigRequest.URL)

    ALLOWED_KEYS = (
        "session_window_ms",
        "cooldown_ms",
        "locks_per_resource",
        "max_concurrency",
        "max_wait_time_minutes",
        "hung_repair_recovery_enabled",
        "hung_repair_stall_threshold_ms",
        "hung_repair_bypass_coordinator_check",
        "hung_repair_force",
    )

    def patch(self, **kwargs):
        body = {key: value for key, value in kwargs.items() if key in ConfigRequest.ALLOWED_KEYS and value is not None}
        headers = {"Content-Type": "application/json"}
        return self.request(ConfigRequest.URL, "PATCH", body=body, headers=headers)


def _create_ssl_context():
    """Build a mutual-TLS SSL context when client-cert env vars are set.

    Returns None when the certificate environment variables are not all
    present, signalling that a plain (non-TLS) connection should be used.
    """
    cert_file = os.getenv("ECCTOOL_CERT_FILE")
    key_file = os.getenv("ECCTOOL_KEY_FILE")
    ca_file = os.getenv("ECCTOOL_CA_FILE")
    if cert_file and key_file and ca_file:
        context = ssl.create_default_context(cafile=ca_file)
        context.load_cert_chain(cert_file, key_file)
        return context
    return None


def _resolve_timeout(timeout):
    """Resolve the request timeout in seconds.

    Precedence: explicit ``timeout`` argument, then the
    ``ECCTOOL_TIMEOUT_SECONDS`` environment variable, then
    ``DEFAULT_TIMEOUT_SECONDS``. Non-positive or non-numeric values fall
    back to the default so a bad setting can never disable the timeout.
    """
    if timeout is None:
        env_value = os.getenv(TIMEOUT_ENV_VAR)
        if env_value is not None:
            try:
                timeout = float(env_value)
            except ValueError:
                timeout = DEFAULT_TIMEOUT_SECONDS
        else:
            timeout = DEFAULT_TIMEOUT_SECONDS
    try:
        timeout = float(timeout)
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT_SECONDS
    if timeout <= 0:
        return DEFAULT_TIMEOUT_SECONDS
    return timeout


class MetricsRequest(RestRequest):
    METRICS = "metrics"

    PROMETHEUS_ACCEPT = "text/plain; version=0.0.4; charset=utf-8"
    OPENMETRICS_ACCEPT = "application/openmetrics-text; version=1.0.0; charset=utf-8"

    def __init__(self, base_url=None, timeout=None):
        RestRequest.__init__(self, base_url, timeout=timeout)

    def get_metrics(self, open_metrics=False):
        """Fetch the raw metrics exposition text.

        Returns the scrape text on success, or a RequestResult on failure
        (e.g. 404 when statistics are disabled on the agent).
        """
        accept = MetricsRequest.OPENMETRICS_ACCEPT if open_metrics else MetricsRequest.PROMETHEUS_ACCEPT
        return self.basic_request(MetricsRequest.METRICS, headers={"Accept": accept})


class RepairSessionsRequest(RestRequest):
    ROOT = "repair-management/repairSessions"

    def __init__(self, base_url=None, timeout=None):
        RestRequest.__init__(self, base_url, timeout=timeout)

    def list_sessions(self, node_id=None):
        request_url = RepairSessionsRequest.ROOT
        if node_id:
            request_url = "{0}?nodeID={1}".format(request_url, node_id)

        result = self.request(request_url)

        if result.is_successful():
            result = result.transform_with_data(new_data=[RepairSession(x) for x in result.data])

        return result

    def fail_session(self, session_id, force=False, node_id=None):
        request_url = "{0}/{1}/fail?force={2}".format(
            RepairSessionsRequest.ROOT, quote(str(session_id)), str(force).lower()
        )
        if node_id:
            request_url = "{0}&nodeID={1}".format(request_url, node_id)

        result = self.request(request_url, "POST")

        if result.is_successful():
            result = result.transform_with_data(new_data=[RepairSession(x) for x in result.data])

        return result
