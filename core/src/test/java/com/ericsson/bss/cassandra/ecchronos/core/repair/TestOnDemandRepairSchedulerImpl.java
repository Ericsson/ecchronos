/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE = new TableReference("keyspace", "table");

    @Mock
    private JmxProxyFactory jmxProxyFactory;

    @Mock
    private ScheduleManager scheduleManager;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private ReplicationState replicationState;

    @Mock
    private Metadata metadata;

    KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);

    TableMetadata tableMetadata = mock(TableMetadata.class);

    @Test
    public void testScheduleRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(keyspaceMetadata);
        when(keyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(tableMetadata);

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        RepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE);
        verify(scheduleManager).schedule(any(ScheduledJob.class));

        assertTableViewExist(repairScheduler, repairJobView);

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleTwoRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(keyspaceMetadata);
        when(keyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(tableMetadata);

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        RepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE);
        RepairJobView repairJobView2 = repairScheduler.scheduleJob(TABLE_REFERENCE);
        verify(scheduleManager, times(2)).schedule(any(ScheduledJob.class));

        assertTableViewExist(repairScheduler, repairJobView, repairJobView2);

        repairScheduler.close();
        verify(scheduleManager, times(2)).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleRepairOnNonExistentKeyspaceTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleRepairOnNonExistentTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(keyspaceMetadata);
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE);
    }

    private void assertTableViewExist(OnDemandRepairScheduler repairScheduler, RepairJobView... expectedViews)
    {
        List<RepairJobView> repairJobViews = repairScheduler.getCurrentRepairJobs();
        assertThat(repairJobViews).containsExactlyInAnyOrder(expectedViews);
    }

    private OnDemandRepairSchedulerImpl.Builder defaultOnDemandRepairSchedulerImplBuilder()
    {
        return OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withReplicationState(replicationState)
                .withMetadata(metadata)
                .withRepairLockType(RepairLockType.VNODE);
    }
}
