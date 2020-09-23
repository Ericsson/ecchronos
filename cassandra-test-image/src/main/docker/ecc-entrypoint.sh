#!/bin/bash
#
# Copyright 2020 Telefonaktiebolaget LM Ericsson
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

set -e

sed -i "s/authenticator: .*/authenticator: PasswordAuthenticator/g" "$CASSANDRA_CONF"/cassandra.yaml
sed -i "s/authorizer: .*/authorizer: CassandraAuthorizer/g" "$CASSANDRA_CONF"/cassandra.yaml

sed -i "s/num_tokens: .*/num_tokens: 16/g" "$CASSANDRA_CONF"/cassandra.yaml
sed -i "s/auto_snapshot: .*/auto_snapshot: false/g" "$CASSANDRA_CONF"/cassandra.yaml

mkdir -p ~/.cassandra

cat <<EOF > ~/.cassandra/cqlshrc
[authentication]
username = cassandra
password = cassandra

EOF

if [ -f /etc/certificates/.keystore ]; then
  sed -i "/client_encryption_options:/{n;s/enabled: false/enabled: true/}" "$CASSANDRA_CONF"/cassandra.yaml

  sed -i "s;keystore: .*;keystore: /etc/certificates/.keystore;g" "$CASSANDRA_CONF"/cassandra.yaml
  sed -i "s/keystore_password: .*/keystore_password: ecctest/g" "$CASSANDRA_CONF"/cassandra.yaml

  sed -i "s;(# )?truststore: .*;truststore: /etc/certificates/.truststore;g" "$CASSANDRA_CONF"/cassandra.yaml
  sed -i "s/(# )?truststore_password: .*/truststore_password: ecctest/g" "$CASSANDRA_CONF"/cassandra.yaml

  sed -i "s/(# )?require_client_auth: false/require_client_auth: true/g" "$CASSANDRA_CONF"/cassandra.yaml
  sed -i "s/(# )?protocol: TLS/protocol: TLSv1.2/g" "$CASSANDRA_CONF"/cassandra.yaml

  cat <<EOF >> ~/.cassandra/cqlshrc
[connection]
hostname = localhost
port = 9042
ssl = true

[ssl]
certfile = /etc/certificates/ca.crt
validate = true
userkey = /etc/certificates/key.pem
usercert = /etc/certificates/cert.crt
version = TLSv1_2
EOF
fi

./docker-entrypoint.sh
