/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.primaryrange;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter.FaultCode;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestPrimaryRangeRepairAlarm
{
    private static final long NOW = TimeUnit.DAYS.toMillis(100);
    private static final long WARNING_MS = TimeUnit.DAYS.toMillis(8);
    private static final long ERROR_MS = TimeUnit.DAYS.toMillis(10);

    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();

    @Mock
    private RepairFaultReporter myFaultReporter;

    private PrimaryRangeRepairAlarm myAlarm;

    @Before
    public void setup()
    {
        RepairConfiguration configuration = RepairConfiguration.newBuilder()
                .withRepairInterval(7, TimeUnit.DAYS)
                .withRepairWarningTime(8, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
                .build();
        myAlarm = new PrimaryRangeRepairAlarm(myFaultReporter, myTableReference, myNodeId, configuration);
    }

    private Map<String, Object> expectedData()
    {
        Map<String, Object> data = new HashMap<>();
        data.put(RepairFaultReporter.FAULT_NODE_ID, myNodeId);
        data.put(RepairFaultReporter.FAULT_KEYSPACE, "keyspace");
        data.put(RepairFaultReporter.FAULT_TABLE, "table");
        data.put(RepairFaultReporter.FAULT_SOURCE, "PRIMARY_RANGE_ELAPSED");
        return data;
    }

    @Test
    public void testNoAlarmBeforeTheWarningTime()
    {
        myAlarm.evaluate(NOW - WARNING_MS + 1, NOW);

        verifyNoMoreInteractions(myFaultReporter);
        assertThat(myAlarm.getRaisedCode()).isNull();
    }

    @Test
    public void testWarningIsRaisedAtTheWarningTime()
    {
        myAlarm.evaluate(NOW - WARNING_MS, NOW);

        verify(myFaultReporter).raise(FaultCode.REPAIR_WARNING, expectedData());
        assertThat(myAlarm.getRaisedCode()).isEqualTo(FaultCode.REPAIR_WARNING);
    }

    @Test
    public void testErrorIsRaisedAtTheErrorTime()
    {
        myAlarm.evaluate(NOW - ERROR_MS, NOW);

        verify(myFaultReporter).raise(FaultCode.REPAIR_ERROR, expectedData());
        verify(myFaultReporter, never()).raise(eq(FaultCode.REPAIR_WARNING), any());
        assertThat(myAlarm.getRaisedCode()).isEqualTo(FaultCode.REPAIR_ERROR);
    }

    @Test
    public void testTheAlarmIsEscalatedFromWarningToError()
    {
        myAlarm.evaluate(NOW - WARNING_MS, NOW);
        myAlarm.evaluate(NOW - WARNING_MS, NOW + (ERROR_MS - WARNING_MS));

        verify(myFaultReporter).raise(FaultCode.REPAIR_WARNING, expectedData());
        verify(myFaultReporter).raise(FaultCode.REPAIR_ERROR, expectedData());
        assertThat(myAlarm.getRaisedCode()).isEqualTo(FaultCode.REPAIR_ERROR);
    }

    @Test
    public void testTheReporterIsOnlyToldOnTransitions()
    {
        myAlarm.evaluate(NOW - WARNING_MS, NOW);
        myAlarm.evaluate(NOW - WARNING_MS, NOW + 1);
        myAlarm.evaluate(NOW - WARNING_MS, NOW + 2);

        verify(myFaultReporter, times(1)).raise(any(), any());
        verify(myFaultReporter, never()).cease(any(), any());
    }

    @Test
    public void testTheAlarmIsCeasedWhenTheTableIsRepaired()
    {
        myAlarm.evaluate(NOW - WARNING_MS, NOW);
        myAlarm.evaluate(NOW, NOW);

        verify(myFaultReporter).cease(FaultCode.REPAIR_WARNING, expectedData());
        assertThat(myAlarm.getRaisedCode()).isNull();
    }

    @Test
    public void testAnErrorIsCeasedWithTheWarningCodeLikeVnodeRepairDoes()
    {
        myAlarm.evaluate(NOW - ERROR_MS, NOW);
        myAlarm.evaluate(NOW, NOW);

        verify(myFaultReporter).cease(FaultCode.REPAIR_WARNING, expectedData());
        verify(myFaultReporter, never()).cease(eq(FaultCode.REPAIR_ERROR), any());
    }

    @Test
    public void testNothingIsCeasedWhenNothingWasRaised()
    {
        myAlarm.evaluate(NOW, NOW);
        myAlarm.evaluate(NOW, NOW + 1);

        verifyNoMoreInteractions(myFaultReporter);
    }

    @Test
    public void testTheAlarmCanBeRaisedAgainAfterItWasCeased()
    {
        myAlarm.evaluate(NOW - WARNING_MS, NOW);
        myAlarm.evaluate(NOW, NOW);
        myAlarm.evaluate(NOW, NOW + WARNING_MS);

        verify(myFaultReporter, times(2)).raise(FaultCode.REPAIR_WARNING, expectedData());
        verify(myFaultReporter, times(1)).cease(any(), any());
    }

    @Test
    public void testTheAlarmDiffersFromTheVnodeAlarmOfTheSameTableAndNode()
    {
        // Vnode repair raises with node, keyspace and table only. An equal map would be one alarm to a reporter that
        // keys on the data.
        Map<String, Object> vnodeData = new HashMap<>();
        vnodeData.put(RepairFaultReporter.FAULT_NODE_ID, myNodeId);
        vnodeData.put(RepairFaultReporter.FAULT_KEYSPACE, "keyspace");
        vnodeData.put(RepairFaultReporter.FAULT_TABLE, "table");

        assertThat(expectedData()).isNotEqualTo(vnodeData);
        assertThat(expectedData().hashCode()).isNotEqualTo(vnodeData.hashCode());
        assertThat(expectedData()).containsEntry(RepairFaultReporter.FAULT_SOURCE,
                PrimaryRangeRepairAlarm.FAULT_SOURCE_PRIMARY_RANGE_ELAPSED);
    }

    @Test
    public void testWithoutAReporterNothingHappens()
    {
        RepairConfiguration configuration = RepairConfiguration.newBuilder()
                .withRepairInterval(7, TimeUnit.DAYS).build();
        PrimaryRangeRepairAlarm alarm = new PrimaryRangeRepairAlarm(null, myTableReference, myNodeId, configuration);

        alarm.evaluate(0, NOW);
        alarm.evaluate(NOW, NOW);

        assertThat(alarm.getRaisedCode()).isNull();
    }
}
