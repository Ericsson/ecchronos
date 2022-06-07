#  Copyright 2022 Telefonaktiebolaget LM Ericsson
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import ssl
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from cassandra.auth import PlainTextAuthProvider


def before_all(context):
    cassandra_address = context.config.userdata.get("cassandra_address")
    assert cassandra_address

    username = context.config.userdata.get("cql_user")
    password = context.config.userdata.get("cql_password")
    auth_provider=None
    if (username and username != '') and (password and password != ''):
        auth_provider = PlainTextAuthProvider(username=username, password=password)
    else:
        print 'Username or password empty, will try to connect without'

    no_tls = context.config.userdata.get("no_tls")
    if no_tls:
        cluster = Cluster([cassandra_address], auth_provider=auth_provider)
    else:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        cluster = Cluster([cassandra_address], ssl_context=ssl_context, auth_provider=auth_provider)
    context.cluster = cluster
    session = cluster.connect()
    context.session = session
    host = cluster.metadata.get_host(cassandra_address)
    context.host_id = host.host_id
