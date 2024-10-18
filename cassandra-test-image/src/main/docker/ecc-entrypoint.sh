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
# Start of for 5.X
sed -i "/^authenticator:/{n;s/class_name : .*/class_name : PasswordAuthenticator/}" "$CASSANDRA_CONF"/cassandra.yaml
# End of for 5.X
sed -i "s/^authorizer: .*/authorizer: CassandraAuthorizer/g" "$CASSANDRA_CONF"/cassandra.yaml

sed -i "s/num_tokens: .*/num_tokens: 16/g" "$CASSANDRA_CONF"/cassandra.yaml
sed -i "s/auto_snapshot: .*/auto_snapshot: false/g" "$CASSANDRA_CONF"/cassandra.yaml

mkdir -p ~/.cassandra

cat <<EOF > ~/.cassandra/cqlshrc
[authentication]
username = cassandra
password = cassandra

EOF

if [ -f /etc/certificates/.keystore ]; then
#
# Setup CQL certificates
#
  sed -i "/client_encryption_options:/{n;s/enabled: false/enabled: true/}" "$CASSANDRA_CONF"/cassandra.yaml

  sed -i "s;keystore: .*;keystore: /etc/certificates/.keystore;g" "$CASSANDRA_CONF"/cassandra.yaml
  # Cassandra 5: 'keystore_password' must not be empty, e.g. remove comment if present
  sed -ri "s/( *)(# *)?keystore_password: .*/\1keystore_password: ecctest/g" "$CASSANDRA_CONF"/cassandra.yaml

  sed -ri "s;(# )?truststore: .*;truststore: /etc/certificates/.truststore;g" "$CASSANDRA_CONF"/cassandra.yaml
  sed -ri "s/(# )?truststore_password: .*/truststore_password: ecctest/g" "$CASSANDRA_CONF"/cassandra.yaml

  sed -ri "s/(# )?require_client_auth: false/require_client_auth: true/g" "$CASSANDRA_CONF"/cassandra.yaml
  sed -ri "s/(# )?protocol: TLS/protocol: TLSv1.2/g" "$CASSANDRA_CONF"/cassandra.yaml

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

#
# Setup JMX certificates
#

# Comment rmi port to choose randomly
  sed -ri "s/(.*jmxremote.rmi.port.*)/#\1/g" "$CASSANDRA_CONF"/cassandra-env.sh

# Enable secure transport
  sed -ri "s/#(.*jmxremote.ssl=true)/\1/g" "$CASSANDRA_CONF"/cassandra-env.sh
  sed -ri "s/#(.*jmxremote.ssl.need_client_auth=true)/\1/g" "$CASSANDRA_CONF"/cassandra-env.sh

# Set protocol
  sed -ri 's/#(.*jmxremote.ssl.enabled.protocols)=.*/\1=TLSv1.2"/g' "$CASSANDRA_CONF"/cassandra-env.sh

# Set keystore/truststore properties
  sed -ri 's;#(.*keyStore)=.*;\1=/etc/certificates/.keystore";g' "$CASSANDRA_CONF"/cassandra-env.sh
  sed -ri 's;#(.*trustStore)=.*;\1=/etc/certificates/.truststore";g' "$CASSANDRA_CONF"/cassandra-env.sh
  sed -ri 's/#(.*keyStorePassword)=.*/\1=ecctest"/g' "$CASSANDRA_CONF"/cassandra-env.sh
  sed -ri 's/#(.*trustStorePassword)=.*/\1=ecctest"/g' "$CASSANDRA_CONF"/cassandra-env.sh

fi

docker-entrypoint.sh
