#!/bin/sh
#
# Copyright 2018 Telefonaktiebolaget LM Ericsson
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

if [ "x$ECCHRONOS_HOME" = "x" ]; then
    ECCHRONOS_HOME="`dirname "$0"`/.."
fi

CLASSPATH="$ECCHRONOS_HOME"/conf/
JVM_ENV=-Decchronos.config="$ECCHRONOS_HOME"/conf/ecc.cfg

for library in "$ECCHRONOS_HOME"/lib/*.jar
do
    CLASSPATH="$CLASSPATH:$library"
done

FOREGROUND=""
PIDFILE=""

while [ $# -gt 0 ]; do
    case "$1" in
        -f)
            FOREGROUND="-f"
            shift
        ;;
        -p)
            PIDFILE="$2"
            shift 2
        ;;
        *)
            echo "Unknown argument '$1'" >&2
            exit 1
        ;;
    esac
done

if [ "$FOREGROUND" = "-f" ]; then
    java $JVM_ENV -cp $CLASSPATH com.ericsson.bss.cassandra.ecchronos.application.ECChronos $FOREGROUND
    [ ! -z "$PIDFILE" ] && echo "$!" > "$PIDFILE"
else
    java $JVM_ENV -cp $CLASSPATH com.ericsson.bss.cassandra.ecchronos.application.ECChronos $@ <&- &
    [ ! -z "$PIDFILE" ] && echo "$!" > "$PIDFILE"
fi
