#
# Copyright 2022 Telefonaktiebolaget LM Ericsson
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

from __future__ import print_function

import ssl
import time
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from cassandra.auth import PlainTextAuthProvider


class Environment:

    cluster = None
    session = None
    host_id = None

    def __init__(self):
        pass


def before_all(context):
    cassandra_address = context.config.userdata.get("cassandra_address")
    assert cassandra_address

    username = context.config.userdata.get("cql_user")
    password = context.config.userdata.get("cql_password")
    auth_provider=None
    if (username and username != '') and (password and password != ''):
        auth_provider = PlainTextAuthProvider(username=username, password=password)

    no_tls = context.config.userdata.get("no_tls")
    if no_tls:
        cluster = Cluster([cassandra_address], auth_provider=auth_provider)
    else:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.load_verify_locations(context.config.userdata.get("cql_client_ca"))
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_cert_chain(
            certfile=context.config.userdata.get("cql_client_cert"),
            keyfile=context.config.userdata.get("cql_client_key"))
        cluster = Cluster([cassandra_address], ssl_context=ssl_context, auth_provider=auth_provider)
    context.environment = Environment()
    context.environment.cluster = cluster
    session = cluster.connect()
    context.environment.session = session
    host = cluster.metadata.get_host(cassandra_address)
    context.environment.host_id = host.host_id


def after_feature(context, feature): # pylint: disable=unused-argument
    wait_for_local_repairs_to_complete(context)
    context.environment.session.execute('TRUNCATE TABLE ecchronos.on_demand_repair_status')
    context.environment.session.execute('TRUNCATE TABLE ecchronos.repair_history')


def wait_for_local_repairs_to_complete(context):
    timeout_seconds = 180
    count = 0
    while count < timeout_seconds:
        uncompleted_repairs = 0
        rows = context.environment.session.execute('SELECT host_id, job_id, status FROM ecchronos.on_demand_repair_status')
        for row in rows:
            if row.host_id == context.environment.host_id:
                if row.status == u'started':
                    uncompleted_repairs += 1
        if uncompleted_repairs < 1:
            break
        count += 1
        time.sleep(1)
    assert count < timeout_seconds, 'All repairs did not finish in {0} seconds'.format(timeout_seconds)
    print('Waiting for repairs to finish took {0} seconds'.format(count))
