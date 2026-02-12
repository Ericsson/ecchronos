#!/bin/bash
#
# Copyright 2024 Telefonaktiebolaget LM Ericsson
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

    if [ ! "$(echo -n "$PEM_ENABLED" | xargs)" = "true" ]; then
    # Setup JMX certificates

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
fi

#
# Setup nodetool SSL properties
#
  cat <<EOF > ~/.cassandra/nodetool-ssl.properties
-Djavax.net.ssl.keyStore=/etc/certificates/.keystore
-Djavax.net.ssl.keyStorePassword=ecctest
-Djavax.net.ssl.trustStore=/etc/certificates/.truststore
-Djavax.net.ssl.trustStorePassword=ecctest
-Dcom.sun.management.jmxremote.ssl.need.client.auth=true
EOF

#
# Setup nodetool status with SSL
#
  cat <<EOF > ~/.cassandra/nodetool-status-ssl.sh
#!/bin/bash
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
nodetool --ssl -u cassandra -pw cassandra status
EOF

chmod 755 ~/.cassandra/nodetool-status-ssl.sh

JOLOKIA_JAR="/opt/jolokia-jvm-agent.jar"

if [ "$(echo -n "$JOLOKIA" | xargs)" = "true" ]; then
    echo "Attempting to download Jolokia jar..."
    curl -L -o "$JOLOKIA_JAR" "https://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-agent-jvm/2.5.0/jolokia-agent-jvm-2.5.0-javaagent.jar"

    JOLOKIA_OPTS="--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED -javaagent:$JOLOKIA_JAR=port=8778,host=*,useSsl=false"

    if [ -n "$JVM_EXTRA_OPTS" ]; then
        export JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS $JOLOKIA_OPTS"
    else
        export JVM_EXTRA_OPTS="$JOLOKIA_OPTS"
    fi
fi

if [ "$(echo -n "$PEM_ENABLED" | xargs)" = "true" ]; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update && apt-get install -y --no-install-recommends nginx openssl
    cat <<EOF > /etc/nginx/nginx.conf
events {}

http {
  log_format debug_format '\$remote_addr - \$remote_user [\$time_local] '
                         '"\$request" \$status \$body_bytes_sent '
                         '"\$http_referer" "\$http_user_agent" '
                         'upstream_response_time=\$upstream_response_time '
                         'request_time=\$request_time '
                         'upstream_addr=\$upstream_addr '
                         'upstream_status=\$upstream_status';

  server {
    listen 8443 ssl;
    ssl_certificate /etc/certificates/pem/servercert.crt;
    ssl_certificate_key /etc/certificates/pem/serverkey.pem;
    ssl_client_certificate /etc/certificates/pem/clientca.crt;

    ssl_verify_client on;
    
    access_log /var/log/nginx/jolokia_access.log debug_format;
    error_log /var/log/nginx/jolokia_error.log debug;

    location /jolokia/ {
      proxy_pass http://127.0.0.1:8778/jolokia/;
      proxy_set_header Host \$host;
      proxy_set_header X-Real-IP \$remote_addr;
      proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
      proxy_set_header Content-Type application/json;
      
      proxy_buffering off;
      proxy_request_buffering off;
      
      proxy_set_header Accept application/json;
      
      add_header Content-Type application/json always;
      
      access_log /var/log/nginx/jolokia_debug.log debug_format;
    }
  }
}
EOF
    nginx
fi

docker-entrypoint.sh
