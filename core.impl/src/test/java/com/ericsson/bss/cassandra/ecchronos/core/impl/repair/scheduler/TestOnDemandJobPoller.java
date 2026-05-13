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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestOnDemandJobPoller
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace1", "table1");

    @Mock
    private OnDemandStatus myOnDemandStatus;
    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private ScheduleManager myScheduleManager;
    @Mock
    private OnDemandRepairJobFactory myJobFactory;
    @Mock
    private OngoingJob myOngoingJob;
    @Mock
    private OnDemandRepairJob myRepairJob;

    private OnDemandJobPoller myPoller;
    private final UUID myNodeId = UUID.randomUUID();
    private final UUID myJobId = UUID.randomUUID();

    @After
    public void teardown()
    {
        if (myPoller != null)
        {
            myPoller.close();
        }
    }

    @Test
    public void testPollSchedulesOngoingJobs()
    {
        when(myOngoingJob.getTableReference()).thenReturn(TABLE_REFERENCE);
        when(myOngoingJob.getHostId()).thenReturn(myNodeId);
        when(myOngoingJob.getJobId()).thenReturn(myJobId);
        when(myRepairJob.getJobId()).thenReturn(myJobId);
        when(myJobFactory.createFromOngoingJob(myOngoingJob)).thenReturn(myRepairJob);

        Map<UUID, Set<OngoingJob>> allOngoingJobs = new HashMap<>();
        Set<OngoingJob> jobs = new HashSet<>();
        jobs.add(myOngoingJob);
        allOngoingJobs.put(myNodeId, jobs);
        when(myOnDemandStatus.getOngoingStartedJobsForAllNodes(myReplicationState)).thenReturn(allOngoingJobs);

        myPoller = OnDemandJobPoller.builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withScheduleManager(myScheduleManager)
                .withJobFactory(myJobFactory)
                .withTryAddJob(job -> true)
                .withRemoveFinishedJobs(() -> { })
                .build();

        verify(myScheduleManager, timeout(1000)).schedule(eq(myNodeId), eq(myRepairJob));
    }

    @Test
    public void testPollSkipsCompletedJobs()
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokens = new HashMap<>();
        LongTokenRange range = new LongTokenRange(1, 2);
        tokens.put(range, ImmutableSet.of(mock(DriverNode.class)));
        Set<LongTokenRange> repairedTokens = new HashSet<>();
        repairedTokens.add(range);

        when(myOngoingJob.getTokens()).thenReturn(tokens);
        when(myOngoingJob.getRepairedTokens()).thenReturn(repairedTokens);
        when(myOngoingJob.getJobId()).thenReturn(myJobId);

        Map<UUID, Set<OngoingJob>> allOngoingJobs = new HashMap<>();
        Set<OngoingJob> jobs = new HashSet<>();
        jobs.add(myOngoingJob);
        allOngoingJobs.put(myNodeId, jobs);
        when(myOnDemandStatus.getOngoingStartedJobsForAllNodes(myReplicationState)).thenReturn(allOngoingJobs);

        myPoller = OnDemandJobPoller.builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withScheduleManager(myScheduleManager)
                .withJobFactory(myJobFactory)
                .withTryAddJob(job -> true)
                .withRemoveFinishedJobs(() -> { })
                .build();

        verify(myOngoingJob, timeout(1000)).finishJob();
    }
}
