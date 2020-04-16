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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;

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

    @Test
    public void testScheduleRepairOnNewTable()
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE);
        verify(scheduleManager, timeout(1000)).schedule(any(ScheduledJob.class));

        assertOneTableViewExist(repairScheduler, TABLE_REFERENCE, RepairConfiguration.DEFAULT);

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    private void assertOneTableViewExist(OnDemandRepairScheduler repairScheduler, TableReference tableReference,
            RepairConfiguration repairConfiguration)
    {
        List<RepairJobView> repairJobViews = repairScheduler.getCurrentRepairJobs();
        assertThat(repairJobViews).hasSize(1);

        RepairJobView repairJobView = repairJobViews.get(0);
        assertThat(repairJobView.getTableReference()).isEqualTo(tableReference);
        assertThat(repairJobView.getRepairConfiguration()).isEqualTo(repairConfiguration);
    }

    private OnDemandRepairSchedulerImpl.Builder defaultOnDemandRepairSchedulerImplBuilder()
    {
        return OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withReplicationState(replicationState)
                .withRepairLockType(RepairLockType.VNODE);
    }
}
