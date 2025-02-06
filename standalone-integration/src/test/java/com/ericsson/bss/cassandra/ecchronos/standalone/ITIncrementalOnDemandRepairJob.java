/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metadata.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.OnDemandRepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import java.util.ArrayList;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.mockito.Mock;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@NotThreadSafe
public class ITIncrementalOnDemandRepairJob extends TestBase
{
    private static final int DEFAULT_JOB_TIMEOUT_IN_SECONDS = 90;

    @Mock
    private static TableRepairMetrics mockTableRepairMetrics;
    private static Metadata myMetadata;
    private static HostStatesImpl myHostStates;
    private static OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;
    private static ScheduleManagerImpl myScheduleManagerImpl;
    private static CASLockFactory myLockFactory;
    private static CassandraMetrics myCassandraMetrics;
    private final Set<TableReference> myRepairs = new HashSet<>();

    @Before
    public void init()
    {
        mockTableRepairMetrics = mock(TableRepairMetrics.class);
        myMetadata = mySession.getMetadata();

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(myHostStates)
                .withConsistencySerial(ConsistencyType.DEFAULT)
                .build();

        Set<UUID> nodeIds = getNativeConnectionProvider().getNodes().keySet();
        List<UUID> nodeIdList = new ArrayList<>(nodeIds);

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withNodeIDList(nodeIdList)
                .withRunInterval(1, TimeUnit.SECONDS)
                .build();

        myCassandraMetrics = new CassandraMetrics(getJmxProxyFactory(),
                Duration.ofSeconds(5), Duration.ofMinutes(30));

        myOnDemandRepairSchedulerImpl = OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withReplicationState(new ReplicationStateImpl(new NodeResolverImpl(mySession), mySession))
                .withSession(mySession)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withOnDemandStatus(new OnDemandStatus(getNativeConnectionProvider()))
                .build();
    }

    @After
    public void clean()
    {
        for (TableReference tableReference : myRepairs)
        {
            mySession.execute(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name")
                    .isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name")
                    .isEqualTo(literal(tableReference.getTable()))
                    .build());
            for (Node node : myMetadata.getNodes().values())
            {
                mySession.execute(QueryBuilder.deleteFrom("ecchronos", "on_demand_repair_status")
                        .whereColumn("host_id")
                        .isEqualTo(literal(node.getHostId()))
                        .build());
            }
        }
        myRepairs.clear();
        reset(mockTableRepairMetrics);
        closeConnections();
    }

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
    public void repairSingleTable() throws Exception
    {
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        Node node = getNodeFromDatacenterOne();
        getJmxConnectionProvider().add(node);
        assertThat(tableReference).isNotNull();
        insertSomeDataAndFlush(tableReference, mySession, node);
        long startTime = System.currentTimeMillis();
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(node.getHostId(), tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(node.getHostId(), tableReference);
                    return maxRepairedAt < startTime && percentRepaired < 100.0d;
                });

        UUID jobId = triggerRepair(tableReference, node);

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(node.getHostId(), tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(node.getHostId(), tableReference);
                    return maxRepairedAt >= startTime && percentRepaired >= 100.0d;
                });

        verifyOnDemandCompleted(jobId, startTime, node.getHostId());
    }

    @Test
    public void repairMultipleTables() throws Exception
    {
        long startTime = System.currentTimeMillis();
        Node node = getNodeFromDatacenterOne();
        getJmxConnectionProvider().add(node);
        TableReference tableReference1 = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        TableReference tableReference2 = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_TWO_NAME);
        assertThat(tableReference1).isNotNull();
        assertThat(tableReference2).isNotNull();
        insertSomeDataAndFlush(tableReference1, mySession, node);
        insertSomeDataAndFlush(tableReference2, mySession, node);
        UUID jobId1 = triggerRepair(tableReference1, node);
        UUID jobId2 = triggerRepair(tableReference2, node);

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);
        verifyOnDemandCompleted(jobId1, startTime, node.getHostId());
        verify(mockTableRepairMetrics).repairSession(eq(tableReference1), any(long.class), any(TimeUnit.class),
                eq(true));
        verifyOnDemandCompleted(jobId2, startTime, node.getHostId());
        verify(mockTableRepairMetrics).repairSession(eq(tableReference2), any(long.class), any(TimeUnit.class),
                eq(true));
    }

    @Test
    public void repairSameTableTwice() throws Exception
    {
        long startTime = System.currentTimeMillis();
        Node node = getNodeFromDatacenterOne();
        getJmxConnectionProvider().add(node);
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        assertThat(tableReference).isNotNull();
        insertSomeDataAndFlush(tableReference, mySession, node);
        UUID jobId1 = triggerRepair(tableReference, node);
        UUID jobId2 = triggerRepair(tableReference, node);

        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS * 2, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);
        verifyOnDemandCompleted(jobId1, startTime, node.getHostId());
        verifyOnDemandCompleted(jobId2, startTime, node.getHostId());
        verify(mockTableRepairMetrics, times(2)).repairSession(eq(tableReference),
                any(long.class), any(TimeUnit.class), eq(true));
    }

    private void verifyOnDemandCompleted(UUID jobId, long startTime, UUID hostId)
    {
        List<OnDemandRepairJobView> completedJobs = myOnDemandRepairSchedulerImpl.getAllRepairJobs(hostId)
                .stream()
                .filter(j -> j.getJobId().equals(jobId))
                .collect(Collectors.toList());
        assertThat(completedJobs).hasSize(1);
        assertThat(completedJobs.get(0).getCompletionTime()).isGreaterThanOrEqualTo(startTime);
    }

    private UUID triggerRepair(TableReference tableReference, Node node) throws EcChronosException
    {
        myRepairs.add(tableReference);
        return myOnDemandRepairSchedulerImpl.scheduleJob(tableReference, RepairType.INCREMENTAL, node.getHostId()).getJobId();
    }
}
