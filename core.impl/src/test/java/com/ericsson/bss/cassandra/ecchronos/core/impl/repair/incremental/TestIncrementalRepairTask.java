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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link IncrementalRepairTask} records its execution in {@code ecchronos.repair_history} through the
 * repair-history session, and that the backward-compatible constructor performs no history tracking.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestIncrementalRepairTask
{
    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();
    private final UUID myJobId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private RepairHistory myRepairHistory;

    @Mock
    private RepairHistory.RepairSession mySession;

    @Mock
    private Node myHistoryNode;

    @Mock
    private CassandraMetrics myCassandraMetrics;

    @Mock
    private DistributedJmxProxy myJmxProxy;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup()
    {
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myRepairHistory.newSession(any(), any(), any(), any(), any(), any())).thenReturn(mySession);
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairType(RepairType.INCREMENTAL)
                .build();
    }

    @Test
    public void testHistoryAwareTaskCreatesSession()
    {
        Set<DriverNode> participants = ImmutableSet.of(mock(DriverNode.class));

        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics,
                myRepairHistory, myHistoryNode, myJobId, participants, null);

        assertThat(task).isNotNull();
        verify(myRepairHistory).newSession(eq(myHistoryNode), eq(myTableReference), eq(myJobId), any(),
                eq(participants), eq(RepairType.INCREMENTAL));
    }

    @Test
    public void testOnExecuteStartsSessionAndOnFinishFinishesSession() throws Exception
    {
        Set<DriverNode> participants = ImmutableSet.of(mock(DriverNode.class));
        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics,
                myRepairHistory, myHistoryNode, myJobId, participants, null);

        invokeProtected(task, "onExecute");
        verify(mySession).start();

        invokeOnFinish(task, RepairStatus.SUCCESS);
        verify(mySession).finish(RepairStatus.SUCCESS);
    }

    @Test
    public void testBackwardCompatibleConstructorDoesNotTrackHistory() throws Exception
    {
        // The legacy constructor uses a no-op session, so no interactions occur with the provided repair history.
        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics);

        invokeProtected(task, "onExecute");
        invokeOnFinish(task, RepairStatus.SUCCESS);

        verifyNoInteractions(myRepairHistory);
        verifyNoInteractions(mySession);
    }

    @Test
    public void testVerifyRepairPassesWhenRepairedStateAdvanced() throws Exception
    {
        // Pending data before (80%), advanced after (95%) -> success (no exception).
        when(myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference)).thenReturn(80.0d, 95.0d);
        when(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(100L, 200L);

        IncrementalRepairTask task = newTaskWithMetrics();

        invokeProtected(task, "onExecute");
        invokeVerifyRepair(task); // should not throw
    }

    @Test
    public void testVerifyRepairFailsWhenPendingDataButNoAdvance() throws Exception
    {
        // Pending data before (80%), no advance after (still 80% / same maxRepairedAt) -> failure.
        when(myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference)).thenReturn(80.0d, 80.0d);
        when(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(100L, 100L);

        IncrementalRepairTask task = newTaskWithMetrics();

        invokeProtected(task, "onExecute");

        assertThatExceptionOfType(ScheduledJobException.class)
                .isThrownBy(() -> invokeVerifyRepairRethrowing(task))
                .withMessageContaining("did not advance");
        // The "did not advance" path must not trigger a cluster-wide session termination, which would abort
        // other tables' running incremental repairs.
        verify(myJmxProxy, org.mockito.Mockito.never()).forceTerminateAllRepairSessions();
    }

    @Test
    public void testVerifyRepairPassesWhenNothingToRepair() throws Exception
    {
        // Fully repaired going in (100%) -> nothing to do, no advance expected, no failure.
        when(myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference)).thenReturn(100.0d, 100.0d);
        when(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(500L, 500L);

        IncrementalRepairTask task = newTaskWithMetrics();

        invokeProtected(task, "onExecute");
        invokeVerifyRepair(task); // should not throw

        // onExecute force-refreshes for the pre-snapshot, but verifyRepair returns early (nothing to repair)
        // so no second refresh happens.
        verify(myCassandraMetrics, org.mockito.Mockito.times(1)).forceRefresh(myNodeId, myTableReference);
    }

    @Test
    public void testVerifyRepairSkippedWhenNoMetricsConfigured() throws Exception
    {
        // Legacy path (no metrics) must not attempt confirmation.
        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics);

        invokeProtected(task, "onExecute");
        invokeVerifyRepair(task); // should not throw

        verifyNoInteractions(myCassandraMetrics);
    }

    @Test
    public void testPreSnapshotForceRefreshesForGroundTruth() throws Exception
    {
        // The "before" reading must be force-refreshed too, so it is measured on the same basis as the
        // post-repair reading. Otherwise a stale-low "before" could mask a repair that did no work.
        when(myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference)).thenReturn(80.0d, 95.0d);
        when(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(100L, 200L);

        IncrementalRepairTask task = newTaskWithMetrics();

        invokeProtected(task, "onExecute");
        invokeVerifyRepair(task);

        // forceRefresh is called once before the pre-snapshot and once before the post reading.
        verify(myCassandraMetrics, org.mockito.Mockito.times(2)).forceRefresh(myNodeId, myTableReference);
    }

    @Test
    public void testVerifyRepairSkipsWhenPostMetricsUnavailable() throws Exception
    {
        // Pending data before (80%); post-repair metric fetch fails and returns sentinel 0/0.0.
        // This must be treated as "cannot confirm" (warn, no failure), not as "did not advance".
        when(myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference)).thenReturn(80.0d, 0.0d);
        when(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(100L, 0L);

        IncrementalRepairTask task = newTaskWithMetrics();

        invokeProtected(task, "onExecute");
        invokeVerifyRepair(task); // must not throw

        verify(myJmxProxy, org.mockito.Mockito.never()).forceTerminateAllRepairSessions();
    }

    @Test
    public void testLostResponseAdoptsFinalizedSession() throws Throwable
    {
        // A started session covering keyspace.table that is FINALIZED must be adopted as a success (issue #1892).
        long now = java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(
                java.util.List.of(session("s-1", "FINALIZED", now, "keyspace.table")));

        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics);

        assertThat(invokeOnLostRepairResponse(task, now - 5).toString()).isEqualTo("ADOPTED_SUCCESS");
    }

    @Test
    public void testLostResponseFailedSessionIsAdoptedFailed() throws Throwable
    {
        long now = java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(
                java.util.List.of(session("s-2", "FAILED", now, "keyspace.table")));

        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics);

        assertThat(invokeOnLostRepairResponse(task, now - 5).toString()).isEqualTo("ADOPTED_FAILED");
    }

    @Test
    public void testLostResponseNoMatchingSessionIsNotAdopted() throws Throwable
    {
        // A session for a different table must not be adopted.
        long now = java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(
                java.util.List.of(session("s-3", "REPAIRING", now, "keyspace.other")));

        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics);
        task.setAdoptionTimings(0L, 0L);

        assertThat(invokeOnLostRepairResponse(task, now - 5).toString()).isEqualTo("NOT_ADOPTED");
    }

    @Test
    public void testLostResponseIgnoresStaleFinalizedSession() throws Throwable
    {
        // Concern (#1892 review): a retained, older FINALIZED session for the same table must NOT be adopted as the
        // just-started repair's success. With only that stale session listed, discovery finds nothing started
        // at/after the request time, so the outcome is NOT_ADOPTED (safe retry) rather than a false success.
        long now = java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        long staleStarted = now - java.util.concurrent.TimeUnit.HOURS.toSeconds(3);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(
                java.util.List.of(session("old-s", "FINALIZED", staleStarted, "keyspace.table")));

        // Short-circuit the discovery retry window so the test does not actually wait.
        IncrementalRepairTask task = new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics);
        task.setAdoptionTimings(0L, 0L);

        assertThat(invokeOnLostRepairResponse(task, now).toString()).isEqualTo("NOT_ADOPTED");
    }

    private static java.util.Map<String, String> session(final String id, final String state, final long started,
            final String tables)
    {
        java.util.Map<String, String> session = new java.util.HashMap<>();
        session.put("SESSION_ID", id);
        session.put("STATE", state);
        session.put("STARTED", Long.toString(started));
        session.put("TABLES", tables);
        return session;
    }

    private Object invokeOnLostRepairResponse(final IncrementalRepairTask task, final long requestEpochSeconds)
            throws Throwable
    {
        java.lang.reflect.Method m = RepairTask.class.getDeclaredMethod(
                "onLostRepairResponse",
                com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy.class,
                long.class);
        m.setAccessible(true);
        try
        {
            return m.invoke(task, myJmxProxy, requestEpochSeconds);
        }
        catch (java.lang.reflect.InvocationTargetException e)
        {
            throw e.getCause();
        }
    }

    private IncrementalRepairTask newTaskWithMetrics()
    {
        Set<DriverNode> participants = ImmutableSet.of(mock(DriverNode.class));
        return new IncrementalRepairTask(
                myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics,
                myRepairHistory, myHistoryNode, myJobId, participants, myCassandraMetrics);
    }

    private void invokeVerifyRepair(final IncrementalRepairTask task) throws Exception
    {
        java.lang.reflect.Method m = RepairTaskAccessor.verifyRepairMethod();
        m.invoke(task, myJmxProxy);
    }

    private void invokeVerifyRepairRethrowing(final IncrementalRepairTask task) throws Throwable
    {
        java.lang.reflect.Method m = RepairTaskAccessor.verifyRepairMethod();
        try
        {
            m.invoke(task, myJmxProxy);
        }
        catch (java.lang.reflect.InvocationTargetException e)
        {
            throw e.getCause();
        }
    }

    private void invokeProtected(final IncrementalRepairTask task, final String method) throws Exception
    {
        java.lang.reflect.Method m = task.getClass().getDeclaredMethod(method);
        m.setAccessible(true);
        m.invoke(task);
    }

    private void invokeOnFinish(final IncrementalRepairTask task, final RepairStatus status) throws Exception
    {
        java.lang.reflect.Method m = task.getClass().getDeclaredMethod("onFinish", RepairStatus.class);
        m.setAccessible(true);
        m.invoke(task, status);
    }

    private static final class RepairTaskAccessor
    {
        private RepairTaskAccessor()
        {
        }

        static java.lang.reflect.Method verifyRepairMethod() throws NoSuchMethodException
        {
            java.lang.reflect.Method m = IncrementalRepairTask.class.getDeclaredMethod(
                    "verifyRepair",
                    com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy.class);
            m.setAccessible(true);
            return m;
        }
    }
}
