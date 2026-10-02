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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestUnifiedScheduleManager
{
    @Mock
    private CASLockFactory myLockFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private RunPolicy myRunPolicy;
    @Mock
    private Node myNode1;
    @Mock
    private Node myNode2;

    private final UUID myNodeId1 = UUID.randomUUID();
    private final UUID myNodeId2 = UUID.randomUUID();

    private UnifiedScheduleManager myScheduler;

    @Before
    public void setup() throws LockException
    {
        when(myNode1.getHostId()).thenReturn(myNodeId1);
        when(myNode2.getHostId()).thenReturn(myNodeId2);
        when(myNativeConnectionProvider.getNodes()).thenReturn(Map.of(myNodeId1, myNode1, myNodeId2, myNode2));
        when(myRunPolicy.validate(any(ScheduledJob.class), any())).thenReturn(-1L);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any())).thenReturn(new DummyLock());

        myScheduler = UnifiedScheduleManager.builder()
                .withRunInterval(1, TimeUnit.SECONDS)
                .withSessionWindow(1, TimeUnit.MINUTES)
                .withLockFactory(myLockFactory)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .build();
        myScheduler.addRunPolicy((job, node) -> myRunPolicy.validate(job, node));
    }

    @After
    public void cleanup()
    {
        myScheduler.close();
    }

    @Test
    public void testRunningNoJobs() throws LockException
    {
        myScheduler.run();

        verify(myLockFactory, never()).tryLock(any(), anyString(), anyInt(), anyMap(), any());
        assertThat(myScheduler.getQueueSize()).isZero();
    }

    @Test
    public void testSingleNodeJobExecutes() throws LockException
    {
        TestJob job = new TestJob(1, myNodeId1);
        myScheduler.schedule(myNodeId1, job);

        myScheduler.run();

        assertThat(job.getTaskRuns()).isEqualTo(1);
        verify(myLockFactory, times(1)).tryLock(any(), anyString(), anyInt(), anyMap(), any());
        // Job stays in the queue (it is not removed by a run).
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);
    }

    @Test
    public void testTasksOnMultipleNodesAllExecuteInOneTick() throws LockException
    {
        // One job whose tasks target two different nodes -> two (job,node) units, both execute.
        TestJob job = new TestJob(myNodeId1, myNodeId2);
        myScheduler.schedule(myNodeId1, job);

        myScheduler.run();

        assertThat(job.getTaskRuns()).isEqualTo(2);
        verify(myLockFactory, times(2)).tryLock(any(), anyString(), anyInt(), anyMap(), any());
    }

    @Test
    public void testJobParkedByRunPolicyDoesNotExecute() throws LockException
    {
        when(myRunPolicy.validate(any(ScheduledJob.class), any())).thenReturn(TimeUnit.HOURS.toMillis(1));

        TestJob job = new TestJob(1, myNodeId1);
        myScheduler.schedule(myNodeId1, job);

        myScheduler.run();

        assertThat(job.getTaskRuns()).isZero();
        verify(myLockFactory, never()).tryLock(any(), anyString(), anyInt(), anyMap(), any());
    }

    @Test
    public void testDescheduleRemovesFromQueue()
    {
        TestJob job = new TestJob(1, myNodeId1);
        myScheduler.schedule(myNodeId1, job);
        assertThat(myScheduler.getQueueSize()).isEqualTo(1);

        myScheduler.deschedule(myNodeId1, job);
        assertThat(myScheduler.getQueueSize()).isZero();
    }

    /**
     * A job whose tasks each target a specific node (via {@link ScheduledTask#getNodeId()}).
     */
    private static final class TestJob extends ScheduledJob
    {
        private final AtomicInteger taskRuns = new AtomicInteger();
        private final List<UUID> taskNodes;

        private TestJob(final int numTasks, final UUID nodeId)
        {
            super(new ConfigurationBuilder().withPriority(Priority.LOW).withRunInterval(1, TimeUnit.SECONDS).build(),
                    nodeId);
            taskNodes = new ArrayList<>();
            for (int i = 0; i < numTasks; i++)
            {
                taskNodes.add(nodeId);
            }
        }

        private TestJob(final UUID... nodeIds)
        {
            super(new ConfigurationBuilder().withPriority(Priority.LOW).withRunInterval(1, TimeUnit.SECONDS).build(),
                    nodeIds[0]);
            taskNodes = new ArrayList<>(List.of(nodeIds));
        }

        private int getTaskRuns()
        {
            return taskRuns.get();
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();
            for (UUID taskNode : taskNodes)
            {
                tasks.add(new NodeTask(taskNode));
            }
            return tasks.iterator();
        }

        private final class NodeTask extends ScheduledTask
        {
            private final UUID myTaskNode;

            private NodeTask(final UUID taskNode)
            {
                myTaskNode = taskNode;
            }

            @Override
            public UUID getNodeId()
            {
                return myTaskNode;
            }

            @Override
            public Set<RepairResource> getRepairResources()
            {
                Set<RepairResource> resources = new HashSet<>();
                resources.add(new RepairResource("dc1", "resource-" + myTaskNode));
                return resources;
            }

            @Override
            public boolean execute(final UUID nodeID)
            {
                taskRuns.incrementAndGet();
                return true;
            }
        }
    }
}
