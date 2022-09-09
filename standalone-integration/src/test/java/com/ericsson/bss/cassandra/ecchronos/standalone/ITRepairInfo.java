/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.EccRepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TokenSubRangeUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
@NotThreadSafe
public class ITRepairInfo extends TestBase
{
    enum RepairHistoryType
    {
        CASSANDRA, ECC
    }
    @Parameterized.Parameters
    public static Collection parameters()
    {
        return Arrays.asList(new Object[][] {
                { RepairHistoryType.CASSANDRA, true },
                { RepairHistoryType.CASSANDRA, false },
                { RepairHistoryType.ECC, true },
                { RepairHistoryType.ECC, false }
        });
    }

    @Parameterized.Parameter
    public RepairHistoryType myRepairHistoryType;

    @Parameterized.Parameter(1)
    public Boolean myRemoteRoutingOption;

    private static CqlSession mySession;

    private static CqlSession myAdminSession;

    private static Node myLocalNode;

    private static DriverNode myLocalDriverNode;

    private static RepairHistoryProvider myRepairHistoryProvider;

    private static NodeResolver myNodeResolver;

    private static TableReferenceFactory myTableReferenceFactory;

    private static RepairStatsProvider myRepairStatsProvider;

    private Set<TableReference> myRepairs = new HashSet<>();

    @Parameterized.BeforeParam
    public static void init(RepairHistoryType repairHistoryType, Boolean remoteRoutingOption) throws IOException
    {
        myRemoteRouting = remoteRoutingOption;
        initialize();

        myAdminSession = getAdminNativeConnectionProvider().getSession();

        myLocalNode = getNativeConnectionProvider().getLocalNode();
        mySession = getNativeConnectionProvider().getSession();

        myTableReferenceFactory = new TableReferenceFactoryImpl(mySession);

        myNodeResolver = new NodeResolverImpl(mySession);
        myLocalDriverNode = myNodeResolver.fromUUID(myLocalNode.getHostId()).orElseThrow(IllegalStateException::new);

        ReplicationState replicationState = new ReplicationStateImpl(myNodeResolver, mySession, myLocalNode);

        EccRepairHistory eccRepairHistory = EccRepairHistory.newBuilder()
                .withReplicationState(replicationState)
                .withLookbackTime(30, TimeUnit.DAYS)
                .withLocalNode(myLocalDriverNode)
                .withSession(mySession)
                .withStatementDecorator(s -> s)
                .build();

        if (repairHistoryType == RepairHistoryType.ECC)
        {
            myRepairHistoryProvider = eccRepairHistory;
        }
        else if (repairHistoryType == RepairHistoryType.CASSANDRA)
        {
            myRepairHistoryProvider = new RepairHistoryProviderImpl(myNodeResolver, mySession, s -> s,
                    TimeUnit.DAYS.toMillis(30));
        }
        else
        {
            throw new IllegalArgumentException("Unknown repair history type for test");
        }
        myRepairStatsProvider = new RepairStatsProviderImpl(
                new VnodeRepairStateFactoryImpl(replicationState, myRepairHistoryProvider, true));
    }

    @After
    public void clean()
    {
        List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();

        Metadata metadata = mySession.getMetadata();
        for (TableReference tableReference : myRepairs)
        {
            stages.add(myAdminSession.executeAsync(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name").isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name").isEqualTo(literal(tableReference.getTable()))
                    .build()));
            for (Node node : metadata.getNodes().values())
            {
                stages.add(myAdminSession.executeAsync(QueryBuilder.deleteFrom("ecchronos", "repair_history")
                        .whereColumn("table_id").isEqualTo(literal(tableReference.getId()))
                        .whereColumn("node_id").isEqualTo(literal(node.getHostId()))
                        .build()));
            }
        }

        for (CompletionStage<AsyncResultSet> stage : stages)
        {
            CompletableFutures.getUninterruptibly(stage);
        }
        myRepairs.clear();
    }

    @Test
    public void repairInfoForRepaired()
    {
        long maxRepairedTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        myRepairs.add(tableReference);
        injectRepairHistory(tableReference, maxRepairedTime);
        //Get repairstats for now - 3 hours to now
        long since = maxRepairedTime - TimeUnit.HOURS.toMillis(1);

        RepairStats repairStatsLocal = myRepairStatsProvider.getRepairStats(tableReference, since,
                System.currentTimeMillis(), true);
        assertThat(repairStatsLocal).isNotNull();
        assertThat(repairStatsLocal.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsLocal.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsLocal.repairedRatio).isGreaterThanOrEqualTo(1.0);

        RepairStats repairStatsClusterWide = myRepairStatsProvider.getRepairStats(tableReference, since,
                System.currentTimeMillis(), false);
        assertThat(repairStatsClusterWide).isNotNull();
        assertThat(repairStatsClusterWide.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsClusterWide.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsClusterWide.repairedRatio).isGreaterThan(0.0);
        assertThat(repairStatsClusterWide.repairedRatio).isLessThan(1.0);
    }

    @Test
    public void repairInfoForRepairedInSubRanges()
    {
        long maxRepairedTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        myRepairs.add(tableReference);
        injectRepairHistory(tableReference, maxRepairedTime, true);
        //Get repairstats for now - 3 hours to now
        long since = maxRepairedTime - TimeUnit.HOURS.toMillis(1);

        RepairStats repairStatsLocal = myRepairStatsProvider.getRepairStats(tableReference, since,
                System.currentTimeMillis(), true);
        assertThat(repairStatsLocal).isNotNull();
        assertThat(repairStatsLocal.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsLocal.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsLocal.repairedRatio).isGreaterThanOrEqualTo(1.0);

        RepairStats repairStatsClusterWide = myRepairStatsProvider.getRepairStats(tableReference, since,
                System.currentTimeMillis(), false);
        assertThat(repairStatsClusterWide).isNotNull();
        assertThat(repairStatsClusterWide.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsClusterWide.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsClusterWide.repairedRatio).isGreaterThan(0.0);
        assertThat(repairStatsClusterWide.repairedRatio).isLessThan(1.0);
    }

    @Test
    public void repairInfoForHalfOfRangesRepaired()
    {
        long maxRepairedTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        myRepairs.add(tableReference);
        Set<TokenRange> expectedRepairedBefore = halfOfTokenRanges(tableReference);
        injectRepairHistory(tableReference, maxRepairedTime, expectedRepairedBefore);
        //Get repairstats for now - 3 hours to now
        long since = maxRepairedTime - TimeUnit.HOURS.toMillis(1);

        RepairStats repairStatsLocal = myRepairStatsProvider.getRepairStats(tableReference, since,
                System.currentTimeMillis(), true);
        assertThat(repairStatsLocal).isNotNull();
        assertThat(repairStatsLocal.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsLocal.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsLocal.repairedRatio).isGreaterThan(0.0);
        assertThat(repairStatsLocal.repairedRatio).isLessThan(1.0);

        RepairStats repairStatsClusterWide = myRepairStatsProvider.getRepairStats(tableReference, since,
                System.currentTimeMillis(), false);
        assertThat(repairStatsClusterWide).isNotNull();
        assertThat(repairStatsClusterWide.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsClusterWide.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsClusterWide.repairedRatio).isGreaterThan(0.0);
        assertThat(repairStatsClusterWide.repairedRatio).isLessThan(repairStatsLocal.repairedRatio);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax)
    {
        injectRepairHistory(tableReference, timestampMax, false);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, boolean splitRanges)
    {
        injectRepairHistory(tableReference, timestampMax,
                mySession.getMetadata().getTokenMap().get().getTokenRanges(tableReference.getKeyspace(), myLocalNode), splitRanges);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, Set<TokenRange> tokenRanges)
    {
        injectRepairHistory(tableReference, timestampMax, tokenRanges, false);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, Set<TokenRange> tokenRanges,
            boolean splitRanges)
    {
        long timestamp = timestampMax - 1;

        for (TokenRange tokenRange : tokenRanges)
        {
            Set<InetAddress> participants = mySession.getMetadata().getTokenMap().get()
                    .getReplicas(tableReference.getKeyspace(), tokenRange).stream()
                    .map(node -> ((InetSocketAddress) node.getEndPoint().resolve()).getAddress())
                    .collect(Collectors.toSet());

            if (splitRanges)
            {
                LongTokenRange longTokenRange = convertTokenRange(tokenRange);
                BigInteger tokensPerRange = longTokenRange.rangeSize().divide(BigInteger.TEN);
                List<LongTokenRange> subRanges = new TokenSubRangeUtil(longTokenRange)
                        .generateSubRanges(tokensPerRange);

                for (LongTokenRange subRange : subRanges)
                {
                    String start = Long.toString(subRange.start);
                    String end = Long.toString(subRange.end);
                    //Make sure to use unique timestamp, otherwise new row will overwrite previous row
                    injectRepairHistory(tableReference, timestamp, participants, start, end);
                    timestamp--;
                }
            }
            else
            {
                String start = Long.toString(((Murmur3Token) tokenRange.getStart()).getValue());
                String end = Long.toString(((Murmur3Token) tokenRange.getEnd()).getValue());
                injectRepairHistory(tableReference, timestamp, participants, start, end);
                timestamp--;
            }
        }
    }

    private void injectRepairHistory(TableReference tableReference, long timestamp, Set<InetAddress> participants,
            String range_begin, String range_end)
    {
        long started_at = timestamp;
        long finished_at = timestamp + 5;

        SimpleStatement statement;

        if (myRepairHistoryType == RepairHistoryType.CASSANDRA)
        {
            statement = QueryBuilder.insertInto("system_distributed", "repair_history")
                    .value("keyspace_name", literal(tableReference.getKeyspace()))
                    .value("columnfamily_name", literal(tableReference.getTable()))
                    .value("participants", literal(participants))
                    .value("coordinator", literal(myLocalDriverNode.getPublicAddress()))
                    .value("id", literal(Uuids.startOf(started_at)))
                    .value("started_at", literal(Instant.ofEpochMilli(started_at)))
                    .value("finished_at", literal(Instant.ofEpochMilli(finished_at)))
                    .value("range_begin", literal(range_begin))
                    .value("range_end", literal(range_end))
                    .value("status", literal("SUCCESS")).build();
        }
        else
        {
            Set<UUID> nodes = participants.stream()
                    .map(myNodeResolver::fromIp)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(DriverNode::getId)
                    .collect(Collectors.toSet());

            statement = QueryBuilder.insertInto("ecchronos", "repair_history")
                    .value("table_id", literal(tableReference.getId()))
                    .value("node_id", literal(myLocalDriverNode.getId()))
                    .value("repair_id", literal(Uuids.startOf(started_at)))
                    .value("job_id", literal(tableReference.getId()))
                    .value("coordinator_id", literal(myLocalDriverNode.getId()))
                    .value("range_begin", literal(range_begin))
                    .value("range_end", literal(range_end))
                    .value("participants", literal(nodes))
                    .value("status", literal("SUCCESS"))
                    .value("started_at", literal(Instant.ofEpochMilli(started_at)))
                    .value("finished_at", literal(Instant.ofEpochMilli(finished_at))).build();
        }
        myAdminSession.execute(statement);
    }

    private Set<TokenRange> halfOfTokenRanges(TableReference tableReference)
    {
        Set<TokenRange> halfOfRanges = new HashSet<>();
        Set<TokenRange> allTokenRanges = mySession.getMetadata().getTokenMap().get()
                .getTokenRanges(tableReference.getKeyspace(), myLocalNode);
        Iterator<TokenRange> iterator = allTokenRanges.iterator();
        for (int i = 0; i < allTokenRanges.size() / 2 && iterator.hasNext(); i++)
        {
            halfOfRanges.add(iterator.next());
        }

        return halfOfRanges;
    }

    private LongTokenRange convertTokenRange(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        return new LongTokenRange(start, end);
    }
}
