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
package com.ericsson.bss.cassandra.ecchronos.core.impl.table;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestTableRepairJob
{
    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long WARNING_IN_DAYS = 7;
    private static final long ERROR_IN_DAYS = 10;

    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private Node myNode;
    @Mock
    private RepairState myRepairState;
    @Mock
    private RepairStateSnapshot mySnapshot;
    @Mock
    private VnodeRepairStates myVnodeRepairStates;
    @Mock
    private RepairHistoryService myRepairHistory;
    @Mock
    private TimeBasedRunPolicy myTimeBasedRunPolicy;

    @Before
    public void setup()
    {
        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myRepairState.getSnapshot()).thenReturn(mySnapshot);
        when(mySnapshot.getVnodeRepairStates()).thenReturn(myVnodeRepairStates);
        when(myVnodeRepairStates.getVnodeRepairStates()).thenReturn(Collections.emptyList());
    }

    private TableRepairJob buildJob()
    {
        RepairConfiguration config = RepairConfiguration.newBuilder()
                .withRepairInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .withRepairWarningTime(WARNING_IN_DAYS, TimeUnit.DAYS)
                .withRepairErrorTime(ERROR_IN_DAYS, TimeUnit.DAYS)
                .build();
        return new TableRepairJob.Builder()
                .withConfiguration(new com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob
                        .ConfigurationBuilder()
                        .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS).build())
                .withNode(myNode)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableReference(myTableReference)
                .withRepairState(myRepairState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(config)
                .withRepairHistory(myRepairHistory)
                .withRepairLockType(RepairLockType.VNODE)
                .withTimeBasedRunPolicy(myTimeBasedRunPolicy)
                .build();
    }

    @Test
    public void testNothingToRepairIsNotReportedBlocked()
    {
        // canRepair() == false means there is nothing to repair. This must NOT surface as BLOCKED: the status
        // classification uses the base runnable() check, not TableRepairJob's canRepair() gate (issue #1822 review).
        when(mySnapshot.canRepair()).thenReturn(false);
        // Recently repaired -> not overdue/late.
        when(mySnapshot.lastCompletedAt()).thenReturn(System.currentTimeMillis());

        TableRepairJob job = buildJob();

        assertThat(job.getView().getStatus()).isNotEqualTo(ScheduledRepairJobView.Status.BLOCKED);
    }

    @Test
    public void testOverdueTakesPrecedenceOverBlocked()
    {
        when(mySnapshot.canRepair()).thenReturn(true);
        when(mySnapshot.lastCompletedAt())
                .thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(ERROR_IN_DAYS));

        TableRepairJob job = buildJob();
        job.setRunnableIn(TimeUnit.HOURS.toMillis(1)); // also parked/blocked

        assertThat(job.getView().getStatus()).isEqualTo(ScheduledRepairJobView.Status.OVERDUE);
    }

    @Test
    public void testLateTakesPrecedenceOverBlocked()
    {
        when(mySnapshot.canRepair()).thenReturn(true);
        when(mySnapshot.lastCompletedAt())
                .thenReturn(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(WARNING_IN_DAYS));

        TableRepairJob job = buildJob();
        job.setRunnableIn(TimeUnit.HOURS.toMillis(1));

        assertThat(job.getView().getStatus()).isEqualTo(ScheduledRepairJobView.Status.LATE);
    }
}
