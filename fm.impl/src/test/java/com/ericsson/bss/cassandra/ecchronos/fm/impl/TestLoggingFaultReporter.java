/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.fm.impl;

import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestLoggingFaultReporter
{
    LoggingFaultReporter loggingFaultReporter = new LoggingFaultReporter();

    @Test
    public void testCeaseAlarms()
    {
        Map<String, Object> data = new HashMap<>();
        data.put(RepairFaultReporter.FAULT_KEYSPACE, "keyspace");
        data.put(RepairFaultReporter.FAULT_TABLE, "table");

        loggingFaultReporter.raise(RepairFaultReporter.FaultCode.REPAIR_ERROR, data);
        assertThat(loggingFaultReporter.alarms.size()).isEqualTo(1);

        loggingFaultReporter.cease(RepairFaultReporter.FaultCode.REPAIR_ERROR, data);
        assertThat(loggingFaultReporter.alarms.size()).isEqualTo(0);
    }

    @Test
    public void testCeaseMultipleAlarms()
    {
        Map<String, Object> data = new HashMap<>();
        data.put(RepairFaultReporter.FAULT_KEYSPACE, "keyspace");
        data.put(RepairFaultReporter.FAULT_TABLE, "table");
        Map<String, Object> anotherData = new HashMap<>();
        anotherData.put(RepairFaultReporter.FAULT_KEYSPACE, "keyspace2");
        anotherData.put(RepairFaultReporter.FAULT_TABLE, "table2");

        loggingFaultReporter.raise(RepairFaultReporter.FaultCode.REPAIR_WARNING, data);
        assertThat(loggingFaultReporter.alarms.size()).isEqualTo(1);

        loggingFaultReporter.cease(RepairFaultReporter.FaultCode.REPAIR_WARNING, anotherData);
        assertThat(loggingFaultReporter.alarms.size()).isEqualTo(1);

        loggingFaultReporter.cease(RepairFaultReporter.FaultCode.REPAIR_ERROR, data);
        assertThat(loggingFaultReporter.alarms.size()).isEqualTo(0);
    }


}
