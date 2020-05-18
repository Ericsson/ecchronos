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
    ECCHRONOS_HOME=$(dirname $(readlink -f $0))/..
fi

cd $ECCHRONOS_HOME

CLASSPATH="$ECCHRONOS_HOME"/conf/

for library in "$ECCHRONOS_HOME"/lib/*.jar
do
    CLASSPATH="$CLASSPATH:$library"
done

# Read user-defined JVM options from jvm.options file
JVM_OPTS_FILE=$ECCHRONOS_HOME/conf/jvm.options
for opt in $(grep "^-" $JVM_OPTS_FILE)
do
  JVM_OPTS="$JVM_OPTS $opt"
done

JVM_OPTS="$JVM_OPTS -Decchronos.config="$ECCHRONOS_HOME"/conf/ecChronos.cfg"

if [ "$1" = "-f" ]; then
    java $JVM_OPTS -cp $CLASSPATH com.ericsson.bss.cassandra.ecchronos.application.ECChronos $@
else
    java $JVM_OPTS -cp $CLASSPATH com.ericsson.bss.cassandra.ecchronos.application.ECChronos $@ <&- 1>&- 2>&- &
fi
