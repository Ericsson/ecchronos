/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
@NotThreadSafe
public class ITIncrementalOnDemandRepairJob extends TestBase
{
    private static final int DEFAULT_JOB_TIMEOUT_IN_SECONDS = 90;

    @Parameterized.Parameters(name = "RemoteRouting: {0}")
    public static List<Boolean> routingOptions()
    {
        return Arrays.asList(true, false);
    }

    @Parameterized.Parameter
    public Boolean myRemoteRoutingOption;

    private static TableRepairMetrics mockTableRepairMetrics;

    private static Metadata myMetadata;

    private static HostStatesImpl myHostStates;

    private static OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;

    private static ScheduleManagerImpl myScheduleManagerImpl;

    private static CASLockFactory myLockFactory;

    private static TableReferenceFactory myTableReferenceFactory;
    private static CassandraMetrics myCassandraMetrics;
    private static CqlSession myAdminSession;

    private Set<TableReference> myRepairs = new HashSet<>();

    @Parameterized.BeforeParam
    public static void init(Boolean remoteRoutingOption) throws IOException
    {
        myRemoteRouting = remoteRoutingOption;
        initialize();

        mockTableRepairMetrics = mock(TableRepairMetrics.class);

        Node localNode = getNativeConnectionProvider().getLocalNode();
        skipIfCassandraVersionLessThan4(localNode);

        CqlSession session = getNativeConnectionProvider().getSession();
        myMetadata = session.getMetadata();

        myTableReferenceFactory = new TableReferenceFactoryImpl(session);

        myHostStates = HostStatesImpl.builder().withRefreshIntervalInMs(1000).withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myLockFactory = CASLockFactory.builder().withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(myHostStates).withStatementDecorator(s -> s).build();

        myScheduleManagerImpl = ScheduleManagerImpl.builder().withLockFactory(myLockFactory)
                .withRunInterval(100, TimeUnit.MILLISECONDS).build();

        myCassandraMetrics = CassandraMetrics.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withReplicatedTableProvider(new ReplicatedTableProviderImpl(localNode, session, myTableReferenceFactory))
                .withInitialDelay(0, TimeUnit.MILLISECONDS)
                .withUpdateDelay(5, TimeUnit.SECONDS)
                .build();

        myOnDemandRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder().withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics).withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(new ReplicationStateImpl(new NodeResolverImpl(session), session, localNode))
                .withSession(session)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withOnDemandStatus(new OnDemandStatus(getNativeConnectionProvider())).build();
        myAdminSession = getAdminNativeConnectionProvider().getSession();
    }

    @After
    public void clean()
    {
        for (TableReference tableReference : myRepairs)
        {
            myAdminSession.execute(
                    QueryBuilder.deleteFrom("system_distributed", "repair_history")
                            .whereColumn("keyspace_name")
                            .isEqualTo(literal(tableReference.getKeyspace())).whereColumn("columnfamily_name")
                            .isEqualTo(literal(tableReference.getTable())).build());
            for (Node node : myMetadata.getNodes().values())
            {
                myAdminSession.execute(
                        QueryBuilder.deleteFrom("ecchronos", "on_demand_repair_status")
                                .whereColumn("host_id")
                                .isEqualTo(literal(node.getHostId())).build());
            }
        }
        myRepairs.clear();
        reset(mockTableRepairMetrics);
    }

    @Parameterized.AfterParam
    public static void closeConnections()
    {
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
    public void repairSingleTable()
            throws EcChronosException, ReflectionException, MalformedObjectNameException, InstanceNotFoundException,
            MBeanException, IOException
    {
        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        insertSomeDataAndFlush(tableReference, myAdminSession);
        long startTime = System.currentTimeMillis();
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(tableReference);
                    return maxRepairedAt < startTime && percentRepaired < 100.0d;
                });

        UUID jobId = triggerRepair(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(tableReference);
                    return maxRepairedAt >= startTime && percentRepaired >= 100.0d;
                });

        verifyOnDemandCompleted(jobId, startTime);
        verify(mockTableRepairMetrics).repairSession(eq(tableReference), any(long.class), any(TimeUnit.class),
                eq(true));
    }

    @Test
    public void repairMultipleTables() throws EcChronosException
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        TableReference tableReference2 = myTableReferenceFactory.forTable("test", "table2");

        UUID jobId1 = triggerRepair(tableReference);
        UUID jobId2 = triggerRepair(tableReference2);

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);
        verifyOnDemandCompleted(jobId1, startTime);
        verify(mockTableRepairMetrics).repairSession(eq(tableReference), any(long.class), any(TimeUnit.class),
                eq(true));
        verifyOnDemandCompleted(jobId2, startTime);
        verify(mockTableRepairMetrics).repairSession(eq(tableReference2), any(long.class), any(TimeUnit.class),
                eq(true));
    }

    @Test
    public void repairSameTableTwice() throws EcChronosException
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        UUID jobId1 = triggerRepair(tableReference);
        UUID jobId2 = triggerRepair(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize() == 0);
        verifyOnDemandCompleted(jobId1, startTime);
        verifyOnDemandCompleted(jobId2, startTime);
        verify(mockTableRepairMetrics, times(2)).repairSession(eq(tableReference),
                any(long.class), any(TimeUnit.class), eq(true));
    }

    private void verifyOnDemandCompleted(UUID jobId, long startTime)
    {
        List<OnDemandRepairJobView> completedJobs = myOnDemandRepairSchedulerImpl.getAllRepairJobs().stream()
                .filter(j -> j.getId().equals(jobId)).collect(Collectors.toList());
        assertThat(completedJobs).hasSize(1);
        assertThat(completedJobs.get(0).getCompletionTime()).isGreaterThanOrEqualTo(startTime);
    }

    private UUID triggerRepair(TableReference tableReference) throws EcChronosException
    {
        myRepairs.add(tableReference);
        return myOnDemandRepairSchedulerImpl.scheduleJob(tableReference, true).getId();
    }
}
