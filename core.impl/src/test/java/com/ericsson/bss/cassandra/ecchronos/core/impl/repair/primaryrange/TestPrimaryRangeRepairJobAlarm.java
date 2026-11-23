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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.TaskExecutionResult;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter.FaultCode;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * When a {@link PrimaryRangeRepairJob} evaluates its alarm. The thresholds and transitions are in
 * {@link TestPrimaryRangeRepairAlarm}.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestPrimaryRangeRepairJobAlarm
{
    private static final long DAY = TimeUnit.DAYS.toMillis(1);

    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private RepairHistory myRepairHistory;
    @Mock
    private RepairHistoryProvider myRepairHistoryProvider;
    @Mock
    private RepairFaultReporter myFaultReporter;
    @Mock
    private Node myNode;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup()
    {
        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myRepairHistoryProvider.lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong()))
                .thenReturn(OptionalLong.empty());
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .withRepairWarningTime(7, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
                .withInitialDelay(0, TimeUnit.MINUTES)
                .withRepairType(RepairType.PRIMARY_RANGE)
                .build();
    }

    private void lastRepairedDaysAgo(final long days)
    {
        when(myRepairHistoryProvider.lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong()))
                .thenReturn(OptionalLong.of(System.currentTimeMillis() - days * DAY));
    }

    private PrimaryRangeRepairJob job(final RepairFaultReporter faultReporter)
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(1, TimeUnit.DAYS)
                .build();
        return new PrimaryRangeRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withReplicationState(myReplicationState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withNode(myNode)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .withFaultReporter(faultReporter)
                .build();
    }

    @Test
    public void testALateTableRaisesTheWarningWhenTheStateIsRefreshed()
    {
        lastRepairedDaysAgo(8);
        PrimaryRangeRepairJob job = job(myFaultReporter);

        job.refreshState();

        ArgumentCaptor<Map<String, Object>> data = ArgumentCaptor.forClass(Map.class);
        verify(myFaultReporter).raise(eq(FaultCode.REPAIR_WARNING), data.capture());
        org.assertj.core.api.Assertions.assertThat(data.getValue())
                .containsEntry(RepairFaultReporter.FAULT_NODE_ID, myNodeId)
                .containsEntry(RepairFaultReporter.FAULT_KEYSPACE, "keyspace")
                .containsEntry(RepairFaultReporter.FAULT_TABLE, "table")
                .containsEntry(RepairFaultReporter.FAULT_SOURCE, "PRIMARY_RANGE_ELAPSED");
    }

    @Test
    public void testAnOverdueTableRaisesTheError()
    {
        lastRepairedDaysAgo(11);

        job(myFaultReporter).refreshState();

        verify(myFaultReporter).raise(eq(FaultCode.REPAIR_ERROR), any());
    }

    @Test
    public void testRefreshingAgainDoesNotRaiseAgain()
    {
        lastRepairedDaysAgo(8);
        PrimaryRangeRepairJob job = job(myFaultReporter);

        job.refreshState();
        job.refreshState();
        job.refreshState();

        verify(myFaultReporter, times(1)).raise(any(), any());
    }

    @Test
    public void testATableThatIsOnTimeHasNoAlarm()
    {
        lastRepairedDaysAgo(2);

        job(myFaultReporter).refreshState();

        verifyNoInteractions(myFaultReporter);
    }

    @Test
    public void testAFirstRepairWithoutHistoryDoesNotAlarm()
    {
        // No history gives a made up last run. It is one interval ago at the most, so never an alarm.
        job(myFaultReporter).refreshState();

        verifyNoInteractions(myFaultReporter);
    }

    @Test
    public void testAnUnreadableHistoryDoesNotAlarm()
    {
        when(myRepairHistoryProvider.lastSuccessfulRepair(any(), any(), any(), anyLong(), anyLong()))
                .thenThrow(new IllegalStateException("history unavailable"));

        job(myFaultReporter).refreshState();

        verifyNoInteractions(myFaultReporter);
    }

    @Test
    public void testASuccessfulRepairCeasesTheAlarm()
    {
        lastRepairedDaysAgo(8);
        PrimaryRangeRepairJob job = job(myFaultReporter);
        job.refreshState();

        job.postExecute(TaskExecutionResult.SUCCESS, job.iterator().next());

        verify(myFaultReporter).cease(eq(FaultCode.REPAIR_WARNING), any());
    }

    @Test
    public void testAFailedRepairKeepsTheAlarm()
    {
        lastRepairedDaysAgo(8);
        PrimaryRangeRepairJob job = job(myFaultReporter);
        job.refreshState();

        job.postExecute(TaskExecutionResult.TERMINAL, job.iterator().next());

        verify(myFaultReporter, never()).cease(any(), any());
    }

    @Test
    public void testARepairFoundInTheHistoryCeasesTheAlarm()
    {
        // For example an on demand repair, that finished since the alarm was raised.
        lastRepairedDaysAgo(8);
        PrimaryRangeRepairJob job = job(myFaultReporter);
        job.refreshState();
        lastRepairedDaysAgo(0);

        job.refreshState();

        verify(myFaultReporter).cease(eq(FaultCode.REPAIR_WARNING), any());
    }

    @Test
    public void testAJobWithoutAFaultReporterWorks()
    {
        lastRepairedDaysAgo(11);
        PrimaryRangeRepairJob job = job(null);

        job.refreshState();
        job.postExecute(TaskExecutionResult.SUCCESS, job.iterator().next());
    }
}
