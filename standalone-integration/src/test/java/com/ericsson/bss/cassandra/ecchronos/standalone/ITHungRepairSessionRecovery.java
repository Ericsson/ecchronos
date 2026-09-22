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
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

/**
 * Verifies that the {@code RepairService} MBean {@code getSessions} operation, as exposed through
 * {@link DistributedJmxProxy#getRepairSessions(UUID)}, returns session maps using the exact keys that
 * {@code HungRepairSessionRecovery} depends on. This guards against silent contract drift on future Cassandra
 * versions: if Cassandra renamed any of these keys, the recovery feature would become a no-op, and only a test
 * against a real node (rather than hand-built maps) can catch that.
 */
@NotThreadSafe
public class ITHungRepairSessionRecovery extends TestBase
{
    private static final int DEFAULT_JOB_TIMEOUT_IN_SECONDS = 90;

    // The keys HungRepairSessionRecovery reads; must match Cassandra's LocalSessionInfo.
    private static final String KEY_SESSION_ID = "SESSION_ID";
    private static final String KEY_STATE = "STATE";
    private static final String KEY_LAST_UPDATE = "LAST_UPDATE";
    private static final String KEY_COORDINATOR = "COORDINATOR";

    private static TableRepairMetrics mockTableRepairMetrics;
    private static Metadata myMetadata;
    private static HostStatesImpl myHostStates;
    private static OnDemandRepairSchedulerImpl myOnDemandRepairSchedulerImpl;
    private static ScheduleManagerImpl myScheduleManagerImpl;
    private static CASLockFactory myLockFactory;
    private static CassandraMetrics myCassandraMetrics;
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

        myCassandraMetrics = new CassandraMetrics(getJmxProxyFactory(),
                Duration.ofSeconds(5), Duration.ofMinutes(30));

        ReplicationStateImpl replicationState = new ReplicationStateImpl(new NodeResolverImpl(getSession()), getSession());
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
    public void getRepairSessionsExposesExpectedKeys() throws Exception
    {
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        Node node = myLocalHost;
        getJmxConnectionProvider().add(node);
        assertThat(tableReference).isNotNull();
        insertSomeDataAndFlush(tableReference, myAdminSession, node);

        // Run one incremental repair so Cassandra persists a consistent (local) repair session that getSessions lists.
        triggerIncrementalRepair(tableReference, node);
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myOnDemandRepairSchedulerImpl.getActiveRepairJobs().isEmpty());
        await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(DEFAULT_JOB_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
                .until(() -> myScheduleManagerImpl.getQueueSize(node.getHostId()) == 0);

        List<Map<String, String>> sessions;
        try (DistributedJmxProxy proxy = getJmxProxyFactory().connect())
        {
            sessions = proxy.getRepairSessions(node.getHostId());
        }

        // The repair completed, so at least one consistent session must be listed by getSessions(true, ...).
        assertThat(sessions)
                .as("getSessions should list the consistent repair session created by the incremental repair")
                .isNotEmpty();

        for (Map<String, String> session : sessions)
        {
            assertThat(session)
                    .as("session map from getSessions must expose the LocalSessionInfo keys the recovery reads")
                    .containsKeys(KEY_SESSION_ID, KEY_STATE, KEY_LAST_UPDATE, KEY_COORDINATOR);

            // LAST_UPDATE must be an absolute epoch timestamp in seconds (parseable, and clearly not a small elapsed
            // value nor milliseconds). This is the semantic HungRepairSessionRecovery relies on.
            long lastUpdateSeconds = Long.parseLong(session.get(KEY_LAST_UPDATE).trim());
            long nowSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
            assertThat(lastUpdateSeconds)
                    .as("LAST_UPDATE should be a recent epoch-seconds timestamp, not elapsed time")
                    .isBetween(nowSeconds - TimeUnit.DAYS.toSeconds(1), nowSeconds + TimeUnit.MINUTES.toSeconds(5));

            assertThat(session.get(KEY_SESSION_ID)).isNotBlank();
            assertThat(session.get(KEY_STATE)).isNotBlank();
            assertThat(session.get(KEY_COORDINATOR)).isNotBlank();
        }
    }

    private void triggerIncrementalRepair(final TableReference tableReference, final Node node) throws EcChronosException
    {
        myRepairs.add(tableReference);
        myOnDemandRepairSchedulerImpl.scheduleJob(tableReference, RepairType.INCREMENTAL, node.getHostId());
    }
}
