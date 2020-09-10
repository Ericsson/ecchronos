#!/bin/bash
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

source variables.sh

echo "Installing virtualenv"

# Install virtualenv and behave
pip install --user virtualenv
virtualenv "$VENV_DIR"
source "$VENV_DIR"/bin/activate

echo "Installing behave"

pip install behave
pip install requests
pip install jsonschema

BASE_DIR="$CURRENT_DIR"/ecchronos-binary-${project.version}
CONF_DIR="$BASE_DIR"/conf
PYLIB_DIR="$BASE_DIR"/pylib

# Change configuration for ecchronos

## Connection
sed "s/#\?connection.native.host=[a-zA-Z0-9.]\+/connection.native.host=$CASSANDRA_IP/g" -i "$CONF_DIR"/ecc.cfg
sed "s/#\?connection.native.port=[0-9]\+/connection.native.port=$CASSANDRA_NATIVE_PORT/g" -i "$CONF_DIR"/ecc.cfg
sed "s/#\?connection.jmx.host=[a-zA-Z0-9.]\+/connection.jmx.host=$CASSANDRA_IP/g" -i "$CONF_DIR"/ecc.cfg
sed "s/#\?connection.jmx.port=[0-9]\+/connection.jmx.port=$CASSANDRA_JMX_PORT/g" -i "$CONF_DIR"/ecc.cfg

# Logback

sed 's;^\(\s*\)\(<appender-ref ref="STDOUT" />\)\s*$;\1<!-- \2 -->;g' -i "$CONF_DIR"/logback.xml


## Scheduler

sed "s/#\?scheduler.run.interval.time=[0-9]\+/scheduler.run.interval.time=1/g" -i "$CONF_DIR"/ecc.cfg
sed "s/#\?scheduler.run.interval.time.unit=seconds\+/scheduler.run.interval.time.unit=seconds/g" -i "$CONF_DIR"/ecc.cfg

cd $PYLIB_DIR

python setup.py install

cd $BASE_DIR

bin/ecc -p $PIDFILE

CHECKS=0
MAX_CHECK=10

echo "Waiting for REST server to start..."
until $(curl --silent --fail --head --output /dev/null http://localhost:8080/repair-management/v1/status); do
    if [ "$CHECKS" -eq "$MAX_CHECK" ]; then
        exit 1
    fi

    echo "..."
    CHECKS=$(($CHECKS+1))
    sleep 2
done

echo "Starting behave"

cd "$TEMP_DIR"

behave --define ecc-status="$BASE_DIR"/bin/ecc-status --define ecc-config="$BASE_DIR"/bin/ecc-config
RETURN=$?

if [ -f $PIDFILE ]; then
    kill $(cat $PIDFILE)
    rm -f $PIDFILE
fi

exit $RETURN