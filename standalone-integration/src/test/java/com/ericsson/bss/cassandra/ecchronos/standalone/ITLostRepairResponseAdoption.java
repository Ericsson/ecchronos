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

package com.ericsson.bss.cassandra.ecchronos.standalone;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metadata.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental.IncrementalRepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.table.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

/**
 * End-to-end coverage for the lost {@code repairAsync} response handling (issue #1892). The transport glitch itself
 * (a dropped HTTP response) cannot be injected deterministically in-process, so this test exercises the recovery path
 * that the glitch triggers against a real Cassandra: after a genuine incremental repair runs, the real consistent
 * session Cassandra persists is adopted and awaited through the production {@link IncrementalRepairTask} adoption
 * hook, using the real {@code getRepairSessions} JMX surface.
 * <p>
 * Two cases are covered against real session data:
 * <ul>
 *   <li>a request made before the repair started adopts the finalized session ({@code ADOPTED_SUCCESS});</li>
 *   <li>a request time after all sessions started is not adopted ({@code NOT_ADOPTED}) — the time bound that guards
 *       against crediting a retained, older session.</li>
 * </ul>
 */
@NotThreadSafe
public class ITLostRepairResponseAdoption extends TestBase
{
    private static final int DEFAULT_JOB_TIMEOUT_IN_SECONDS = 90;

    private static TableRepairMetrics mockTableRepairMetrics;
    private static Metadata myMetadata;
    private static HostStatesImpl myHostStates;
    private static OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;
    private static ScheduleManagerImpl myScheduleManagerImpl;
    private static CASLockFactory myLockFactory;
    private static CqlSession myAdminSession;
    private static TableReferenceFactory myTableReferenceFactory;
    private static Node myLocalHost;

    private final Set<TableReference> myRepairs = new HashSet<>();

    @Before
    public void init() throws IOException
    {
        initialize();
        myLocalHost = getNode();
        mockTableRepairMetrics = mock(TableRepairMetrics.class);
        myMetadata = getSession().getMetadata();
        myTableReferenceFactory = new TableReferenceFactoryImpl(getSession());

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withConsistencySerial(ConsistencyType.SERIAL)
                .build();

        List<UUID> localNodeIdList = Collections.singletonList(myLocalHost.getHostId());
        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withNodeIDList(localNodeIdList)
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withRunInterval(1, TimeUnit.SECONDS)
                .build();

        ReplicationStateImpl replicationState =
                new ReplicationStateImpl(new NodeResolverImpl(getSession()), getSession());
        RepairHistoryService repairHistoryService = new RepairHistoryService(getSession(), replicationState,
                new NodeResolverImpl(getSession()), TimeUnit.DAYS.toMillis(30));

        myOnDemandRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(replicationState)
                .withSession(getSession())
                .withRepairConfigurationFunction(RepairConfiguration.DEFAULT)
                .withRepairHistory(repairHistoryService)
                .withOnDemandStatus(new OnDemandStatus(getNativeConnectionProvider()))
                .build();
        myAdminSession = getAdminNativeConnectionProvider().getCqlSession();
        myScheduleManagerImpl.createScheduleFutureForNodeIDList(getNativeConnectionProvider().getNodes().keySet());
    }

    @After
    public void clean()
    {
        for (TableReference tableReference : myRepairs)
        {
            myAdminSession.execute(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name")
                    .isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name")
                    .isEqualTo(literal(tableReference.getTable()))
                    .build());
            for (Node node : myMetadata.getNodes().values())
            {
                myAdminSession.execute(QueryBuilder.deleteFrom("ecchronos", "on_demand_repair_status")
                        .whereColumn("host_id")
                        .isEqualTo(literal(node.getHostId()))
                        .build());
            }
            dropRepairTable(myAdminSession, tableReference.getTable());
        }
        myRepairs.clear();
        reset(mockTableRepairMetrics);
        if (myHostStates != null)
        {
            myHostStates.close();
        }
        if (myOnDemandRepairSchedulerImpl != null)
        {
            myOnDemandRepairSchedulerImpl.close();
        }
        if (myScheduleManagerImpl != null)
        {
            myScheduleManagerImpl.close();
        }
        if (myLockFactory != null)
        {
            myLockFactory.close();
        }
    }

    @Test
    public void adoptsFinalizedSessionAfterLostResponse() throws Exception
    {
        TableReference tableReference = myTableReferenceFactory.forTable(
                TEST_KEYSPACE, createUniqueRepairTable(myAdminSession, getSession(), "lostresponse"));
        Node node = myLocalHost;
        getJmxConnectionProvider().add(node);
        assertThat(tableReference).isNotNull();
        insertSomeDataAndFlush(tableReference, myAdminSession, node);

        // Capture the request time just before the repair starts, as RepairTask does before calling repairAsync.
        long requestEpochSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

        // Run a real incremental repair so Cassandra persists a consistent session that getSessions will list and
        // which will finalize. This stands in for the repair that would have started had the response not been lost.
        runIncrementalRepairToCompletion(tableReference, node);

        IncrementalRepairTask task = new IncrementalRepairTask(
                node.getHostId(), getJmxProxyFactory(), tableReference, incrementalConfig(), mockTableRepairMetrics);

        try (DistributedJmxProxy proxy = getJmxProxyFactory().connect())
        {
            // A request made before the repair started must adopt the real finalized session.
            assertThat(invokeOnLostRepairResponse(task, proxy, requestEpochSeconds).toString())
                    .as("the finalized consistent session must be adopted as a success")
                    .isEqualTo("ADOPTED_SUCCESS");

            // A request time after every session started must NOT adopt a retained session (the time-bound guard
            // against falsely crediting an older finalized session). See the #1892 review hardening.
            long futureRequest = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
                    + TimeUnit.HOURS.toSeconds(1);
            assertThat(invokeOnLostRepairResponse(task, proxy, futureRequest).toString())
                    .as("a session that started before the request time must not be adopted")
                    .isEqualTo("NOT_ADOPTED");
        }
    }

    private void runIncrementalRepairToCompletion(final TableReference tableReference, final Node node)
            throws EcChronosException
    {
        myRepairs.add(tableReference);
        myOnDemandRepairSchedulerImpl.scheduleJob(tableReference, RepairType.INCREMENTAL, node.getHostId());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);
    }

    private static RepairConfiguration incrementalConfig()
    {
        return RepairConfiguration.newBuilder()
                .withRepairType(RepairType.INCREMENTAL)
                .build();
    }

    /**
     * Invoke the protected adoption hook on the real task. Reflection keeps the production API free of a test-only
     * accessor; the hook shrinks its poll/discovery windows via setAdoptionTimings so this does not wait long.
     */
    private Object invokeOnLostRepairResponse(final IncrementalRepairTask task, final DistributedJmxProxy proxy,
            final long requestEpochSeconds) throws Exception
    {
        Method timings = IncrementalRepairTask.class.getDeclaredMethod(
                "setAdoptionTimings", long.class, long.class);
        timings.setAccessible(true);
        timings.invoke(task, TimeUnit.SECONDS.toMillis(1), TimeUnit.SECONDS.toMillis(5));

        Method hook = Class.forName(
                "com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask")
                .getDeclaredMethod("onLostRepairResponse", DistributedJmxProxy.class, long.class);
        hook.setAccessible(true);
        return hook.invoke(task, proxy, requestEpochSeconds);
    }
}
