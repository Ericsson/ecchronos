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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestIncrementalRepairStallGuard
{
    private static final int THRESHOLD = 3;

    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();

    @Mock
    private RepairFaultReporter myFaultReporter;

    @Mock
    private CassandraMetrics myCassandraMetrics;

    private IncrementalRepairStallGuard myGuard;

    @Before
    public void setup()
    {
        myGuard = new IncrementalRepairStallGuard(
                myFaultReporter, myCassandraMetrics, myTableReference, myNodeId, THRESHOLD);
    }

    private void withMetrics(final double percentRepaired, final long maxRepairedAt)
    {
        when(myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference)).thenReturn(percentRepaired);
        when(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(maxRepairedAt);
    }

    @Test
    public void testAdvancingRepairNeverRaises()
    {
        withMetrics(50.0d, 100L);
        myGuard.onRepairCycleCompleted();
        withMetrics(60.0d, 200L);
        myGuard.onRepairCycleCompleted();
        withMetrics(70.0d, 300L);
        myGuard.onRepairCycleCompleted();
        withMetrics(80.0d, 400L);
        myGuard.onRepairCycleCompleted();

        verify(myFaultReporter, never()).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMap());
    }

    @Test
    public void testStalledWithPendingDataRaisesAtThreshold()
    {
        // Same pending state repeated: first observation establishes baseline, then stalls accumulate.
        withMetrics(80.0d, 100L);
        myGuard.onRepairCycleCompleted(); // baseline (first observation) — no stall counted

        for (int i = 0; i < THRESHOLD - 1; i++)
        {
            myGuard.onRepairCycleCompleted(); // stalled cycles 1..THRESHOLD-1
            verify(myFaultReporter, never()).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMap());
        }

        myGuard.onRepairCycleCompleted(); // reaches THRESHOLD
        verify(myFaultReporter, times(1)).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMap());
    }

    @Test
    public void testAlarmRaisedOnlyOnceWhileStalled()
    {
        withMetrics(80.0d, 100L);
        for (int i = 0; i < THRESHOLD + 3; i++)
        {
            myGuard.onRepairCycleCompleted();
        }
        // Raised once, not on every subsequent stalled cycle.
        verify(myFaultReporter, times(1)).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMap());
    }

    @Test
    public void testAdvanceAfterStallCeasesAlarm()
    {
        withMetrics(80.0d, 100L);
        for (int i = 0; i < THRESHOLD + 1; i++)
        {
            myGuard.onRepairCycleCompleted();
        }
        verify(myFaultReporter, times(1)).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMap());

        // Progress resumes -> alarm ceased and counter reset.
        withMetrics(90.0d, 200L);
        myGuard.onRepairCycleCompleted();
        verify(myFaultReporter, times(1)).cease(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMap());
    }

    @Test
    public void testNothingToRepairNeverRaises()
    {
        // Fully repaired every cycle: repaired_at legitimately does not advance, must not be flagged.
        withMetrics(100.0d, 500L);
        for (int i = 0; i < THRESHOLD + 2; i++)
        {
            myGuard.onRepairCycleCompleted();
        }
        verify(myFaultReporter, never()).raise(eq(RepairFaultReporter.FaultCode.REPAIR_WARNING), anyMap());
    }

    @Test
    public void testForcesFreshMetricReadEachCycle()
    {
        withMetrics(80.0d, 100L);
        myGuard.onRepairCycleCompleted();
        myGuard.onRepairCycleCompleted();

        verify(myCassandraMetrics, times(2)).forceRefresh(myNodeId, myTableReference);
    }

    @Test
    public void testNoFaultReporterIsNoOp()
    {
        IncrementalRepairStallGuard guard = new IncrementalRepairStallGuard(
                null, myCassandraMetrics, myTableReference, myNodeId, THRESHOLD);

        guard.onRepairCycleCompleted();

        verifyNoInteractions(myCassandraMetrics);
    }

    @Test
    public void testStallAlarmDoesNotCollideWithTimeBasedWarning()
    {
        // A reporter that keys active alarms on the data map, mirroring LoggingFaultReporter. The stall guard's
        // data must not share a key with the time-based REPAIR_WARNING raised by AlarmPostUpdateHook for the same
        // {node, keyspace, table}, otherwise one source's cease() would clear the other's alarm.
        Map<Integer, RepairFaultReporter.FaultCode> alarms = new java.util.HashMap<>();
        RepairFaultReporter keyingReporter = new RepairFaultReporter()
        {
            @Override
            public void raise(final FaultCode faultCode, final Map<String, Object> data)
            {
                alarms.put(data.hashCode(), faultCode);
            }

            @Override
            public void cease(final FaultCode faultCode, final Map<String, Object> data)
            {
                alarms.remove(data.hashCode());
            }
        };

        // Simulate AlarmPostUpdateHook having raised a time-based REPAIR_WARNING for the same table.
        Map<String, Object> timeBasedData = new java.util.HashMap<>();
        timeBasedData.put(RepairFaultReporter.FAULT_NODE_ID, myNodeId);
        timeBasedData.put(RepairFaultReporter.FAULT_KEYSPACE, myTableReference.getKeyspace());
        timeBasedData.put(RepairFaultReporter.FAULT_TABLE, myTableReference.getTable());
        keyingReporter.raise(RepairFaultReporter.FaultCode.REPAIR_WARNING, timeBasedData);
        int timeBasedKey = timeBasedData.hashCode();

        // Drive the stall guard to raise its own alarm.
        IncrementalRepairStallGuard guard = new IncrementalRepairStallGuard(
                keyingReporter, myCassandraMetrics, myTableReference, myNodeId, THRESHOLD);
        withMetrics(80.0d, 100L);
        for (int i = 0; i < THRESHOLD + 1; i++)
        {
            guard.onRepairCycleCompleted();
        }

        // Both alarms coexist under distinct keys — the FAULT_SOURCE marker keeps the stall guard's data map
        // from colliding with the time-based warning, even though both use FaultCode.REPAIR_WARNING.
        assertThat(alarms).hasSize(2);
        assertThat(alarms).containsKey(timeBasedKey);

        // When the stall clears, only the stall alarm is ceased; the time-based warning remains.
        withMetrics(90.0d, 200L);
        guard.onRepairCycleCompleted();
        assertThat(alarms).hasSize(1);
        assertThat(alarms).containsKey(timeBasedKey);
    }
}
