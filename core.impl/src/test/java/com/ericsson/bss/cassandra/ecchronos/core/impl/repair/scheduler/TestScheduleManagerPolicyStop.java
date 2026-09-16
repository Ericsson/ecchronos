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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.DummyLock;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Verifies that a repair job stopped by a run policy mid-session (issue #1821) is parked only until the rejection
 * window ends and does not go through the failure path, so it resumes automatically when the policy clears rather
 * than remaining permanently BLOCKED.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleManagerPolicyStop
{
    @Mock
    private CASLockFactory myLockFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private Node node1;

    private final UUID nodeID1 = UUID.randomUUID();
    private final Collection<UUID> myNodes = Arrays.asList(nodeID1);

    private ScheduleManagerImpl myScheduler;

    /** When true, the run policy allows selection (-1) but rejects on the subsequent (re-check) call. */
    private volatile boolean myRejectMidSession = false;
    private volatile long myRejectWindowMs = 200L;
    private final AtomicInteger myValidateCalls = new AtomicInteger(0);

    @Before
    public void startup() throws LockException
    {
        Map<UUID, Node> nodeMap = Map.of(nodeID1, node1);
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodeMap);
        when(myLockFactory.tryLock(any(), anyString(), anyInt(), anyMap(), any())).thenReturn(new DummyLock());
        myScheduler = ScheduleManagerImpl.builder()
                .withNodeIDList(myNodes)
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .withSessionWindow(5, TimeUnit.MINUTES)
                .build();
        // Run policy: allows the job to be selected (-1 on the first call each cycle) but, when
        // myRejectMidSession is set, rejects on the scheduler's post-execute re-check (the second call),
        // mimicking a run policy that becomes active during the session.
        myScheduler.addRunPolicy((job, node) ->
        {
            int call = myValidateCalls.incrementAndGet();
            if (myRejectMidSession && call > 1)
            {
                return myRejectWindowMs;
            }
            return -1L;
        });
        myScheduler.createScheduleFutureForNodeIDList(myNodes);
    }

    @After
    public void cleanup()
    {
        myScheduler.close();
    }

    @Test
    public void testPolicyStoppedJobParksForRejectWindowNotFixedBackoff()
    {
        PolicyStoppedJob job = new PolicyStoppedJob(nodeID1);
        myScheduler.schedule(nodeID1, job);

        // Selection allowed, but the run policy rejects on the post-execute re-check with a short window.
        myRejectMidSession = true;
        myRejectWindowMs = 200L;
        myValidateCalls.set(0);
        myScheduler.run(nodeID1);

        // The task ran (selection passed) then was treated as a policy-stop, not a failure.
        assertThat(job.getTaskRuns()).isEqualTo(1);
        assertThat(job.getState()).isNotEqualTo(ScheduledJob.State.FAILED);
        assertThat(job.getState()).isEqualTo(ScheduledJob.State.PARKED);
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
    }

    @Test
    public void testPolicyStoppedJobRecoversWhenPolicyClears() throws InterruptedException
    {
        PolicyStoppedJob job = new PolicyStoppedJob(nodeID1);
        myScheduler.schedule(nodeID1, job);

        myRejectMidSession = true;
        myRejectWindowMs = 2000L;
        myValidateCalls.set(0);
        myScheduler.run(nodeID1);
        assertThat(job.getTaskRuns()).isEqualTo(1);
        assertThat(job.getState()).isEqualTo(ScheduledJob.State.PARKED);

        // Clear the policy and let the task succeed, then make the job immediately runnable again
        // (the reject-window park would otherwise still be in the future).
        myRejectMidSession = false;
        job.setSucceed(true);
        job.setRunnableIn(0L);
        myValidateCalls.set(0);

        myScheduler.run(nodeID1);
        // Policy cleared and job runnable again -> executed successfully.
        assertThat(job.getTaskRuns()).isEqualTo(2);
    }

    @Test
    public void testRepeatedPolicyStopNeverMarksFailed() throws InterruptedException
    {
        PolicyStoppedJob job = new PolicyStoppedJob(nodeID1);
        myScheduler.schedule(nodeID1, job);
        myRejectMidSession = true;
        myRejectWindowMs = 1L;

        // Many cycles while the policy keeps rejecting mid-session; must never escalate to FAILED.
        for (int i = 0; i < 10; i++)
        {
            Thread.sleep(3L); // let the tiny park expire so the job is selectable each cycle
            myValidateCalls.set(0);
            myScheduler.run(nodeID1);
            assertThat(job.getState()).isNotEqualTo(ScheduledJob.State.FAILED);
        }
        assertThat(myScheduler.getQueueSize(nodeID1)).isEqualTo(1);
    }

    private static final class PolicyStoppedJob extends ScheduledJob
    {
        private final AtomicInteger myTaskRuns = new AtomicInteger(0);
        private volatile boolean mySucceed = false;

        PolicyStoppedJob(final UUID nodeId)
        {
            super(new ConfigurationBuilder()
                    .withPriority(Priority.LOW)
                    .withRunInterval(1, TimeUnit.MILLISECONDS)
                    .build(), nodeId);
        }

        void setSucceed(final boolean succeed)
        {
            mySucceed = succeed;
        }

        int getTaskRuns()
        {
            return myTaskRuns.get();
        }

        @Override
        public Iterator<ScheduledTask> iterator()
        {
            List<ScheduledTask> tasks = new ArrayList<>();
            tasks.add(new PolicyStoppedTask());
            return tasks.iterator();
        }

        private final class PolicyStoppedTask extends ScheduledTask
        {
            @Override
            public boolean execute(final UUID nodeID)
            {
                myTaskRuns.incrementAndGet();
                // Mimic RepairGroup.execute(): returns false when stopped by policy (or a real failure).
                return mySucceed;
            }
        }
    }
}
