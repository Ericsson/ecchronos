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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairSchedulerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ConsistencyType;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
@NotThreadSafe
public class ITIncrementalSchedules extends TestBase
{
    private static final Logger LOG = LoggerFactory.getLogger(ITIncrementalSchedules.class);
    private static final int DEFAULT_SCHEDULE_TIMEOUT_IN_SECONDS = 90;
    private static final int CASSANDRA_METRICS_UPDATE_IN_SECONDS = 5;

    @Parameterized.Parameters(name = "RemoteRouting: {0}")
    public static List<Boolean> routingOptions()
    {
        return Arrays.asList(true, false);
    }

    @Parameterized.Parameter
    public Boolean myRemoteRoutingOption;

    private static RepairFaultReporter mockFaultReporter;
    private static TableRepairMetrics mockTableRepairMetrics;
    private static CqlSession myAdminSession;
    private static HostStatesImpl myHostStates;
    private static RepairSchedulerImpl myRepairSchedulerImpl;
    private static ScheduleManagerImpl myScheduleManagerImpl;
    private static CASLockFactory myLockFactory;
    private static RepairConfiguration myRepairConfiguration;
    private static TableReferenceFactory myTableReferenceFactory;
    private static CassandraMetrics myCassandraMetrics;

    private Set<TableReference> myRepairs = new HashSet<>();

    @Parameterized.BeforeParam
    public static void init(Boolean remoteRoutingOption) throws IOException
    {
        myRemoteRouting = remoteRoutingOption;
        initialize();

        Node localNode = getNativeConnectionProvider().getLocalNode();

        mockFaultReporter = mock(RepairFaultReporter.class);
        mockTableRepairMetrics = mock(TableRepairMetrics.class);

        myAdminSession = getAdminNativeConnectionProvider().getSession();

        CqlSession session = getNativeConnectionProvider().getSession();
        myTableReferenceFactory = new TableReferenceFactoryImpl(session);

        myHostStates = HostStatesImpl.builder()
                .withRefreshIntervalInMs(1000)
                .withJmxProxyFactory(getJmxProxyFactory())
                .build();

        myLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(getNativeConnectionProvider())
                .withHostStates(myHostStates)
                .withStatementDecorator(s -> s)
                .withConsistencySerial(ConsistencyType.DEFAULT)
                .build();

        myScheduleManagerImpl = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withRunInterval(1, TimeUnit.SECONDS)
                .build();

        myCassandraMetrics = new CassandraMetrics(getJmxProxyFactory(),
                Duration.ofSeconds(CASSANDRA_METRICS_UPDATE_IN_SECONDS), Duration.ofMinutes(30));

        myRepairSchedulerImpl = RepairSchedulerImpl.builder()
                .withJmxProxyFactory(getJmxProxyFactory())
                .withTableRepairMetrics(mockTableRepairMetrics)
                .withFaultReporter(mockFaultReporter)
                .withScheduleManager(myScheduleManagerImpl)
                .withRepairLockType(RepairLockType.VNODE)
                .withCassandraMetrics(myCassandraMetrics)
                .withReplicationState(new ReplicationStateImpl(new NodeResolverImpl(session), session, localNode))
                .build();

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.SECONDS)
                .withRepairType(RepairOptions.RepairType.INCREMENTAL)
                .build();
    }

    @After
    public void clean()
    {
        List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();
        for (TableReference tableReference : myRepairs)
        {
            myRepairSchedulerImpl.removeConfiguration(tableReference);

            stages.add(myAdminSession.executeAsync(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name").isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name").isEqualTo(literal(tableReference.getTable()))
                    .build()));
        }
        for (CompletionStage<AsyncResultSet> stage : stages)
        {
            CompletableFutures.getUninterruptibly(stage);
        }
        myRepairs.clear();
        reset(mockTableRepairMetrics);
        reset(mockFaultReporter);
    }

    @Parameterized.AfterParam
    public static void closeConnections()
    {
        if (myHostStates != null)
        {
            myHostStates.close();
        }
        if (myRepairSchedulerImpl != null)
        {
            myRepairSchedulerImpl.close();
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
            throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, MBeanException,
            IOException
    {
        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        insertSomeDataAndFlush(tableReference, myAdminSession);
        long startTime = System.currentTimeMillis();

        // Wait for metrics to be updated, wait at least 3 times the update time for metrics (worst case scenario)
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(CASSANDRA_METRICS_UPDATE_IN_SECONDS*3, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(tableReference);
                    LOG.info("Waiting for metrics to be updated, percentRepaired: {} maxRepairedAt: {}",
                            percentRepaired, maxRepairedAt);
                    return maxRepairedAt < startTime && percentRepaired < 100.0d;
                });

        // Create a schedule
        addSchedule(tableReference);
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_SCHEDULE_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> getSchedule(tableReference).isPresent());

        await().pollInterval(1, TimeUnit.SECONDS).atMost(DEFAULT_SCHEDULE_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() ->
                {
                    double percentRepaired = myCassandraMetrics.getPercentRepaired(tableReference);
                    long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(tableReference);
                    LOG.info("Waiting for schedule to run, percentRepaired: {} maxRepairedAt: {}",
                            percentRepaired, maxRepairedAt);
                    return maxRepairedAt >= startTime && percentRepaired >= 100.0d;
                });
        verify(mockFaultReporter, never())
                .raise(any(RepairFaultReporter.FaultCode.class), anyMap());
        verify(mockTableRepairMetrics).repairSession(eq(tableReference),
                any(long.class), any(TimeUnit.class), eq(true));
        Optional<ScheduledRepairJobView> view = getSchedule(tableReference);
        assertThat(view).isPresent();
        assertThat(view.get().getStatus()).isEqualTo(ScheduledRepairJobView.Status.COMPLETED);
        assertThat(view.get().getCompletionTime()).isGreaterThanOrEqualTo(startTime);
        assertThat(view.get().getProgress()).isGreaterThanOrEqualTo(1.0d);
    }

    private void addSchedule(TableReference tableReference)
    {
        if (myRepairs.add(tableReference))
        {
            myRepairSchedulerImpl.putConfigurations(tableReference, Collections.singleton(myRepairConfiguration));
        }
    }

    private Optional<ScheduledRepairJobView> getSchedule(TableReference tableReference)
    {
        return myRepairSchedulerImpl.getCurrentRepairJobs().stream()
                .filter(s -> s.getRepairConfiguration().equals(myRepairConfiguration))
                .filter(s -> s.getTableReference().equals(tableReference))
                .findFirst();
    }
}
