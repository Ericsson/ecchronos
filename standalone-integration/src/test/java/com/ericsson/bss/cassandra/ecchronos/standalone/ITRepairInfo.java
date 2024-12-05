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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metadata.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.RepairStatsProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStateFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.state.TokenSubRangeUtil;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.CassandraRepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
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

@RunWith (Parameterized.class)
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
                                              { RepairHistoryType.CASSANDRA },
                                              { RepairHistoryType.ECC }
        });
    }

    @Parameterized.Parameter
    public RepairHistoryType myRepairHistoryType;

    private static DriverNode myLocalDriverNode;

    private static NodeResolver myNodeResolver;

    private static RepairStatsProvider myRepairStatsProvider;

    private final Set<TableReference> myRepairs = new HashSet<>();

    @Parameterized.BeforeParam
    public static void init(RepairHistoryType repairHistoryType)
    {
        Node node = getNodeFromDatacenterOne();
        myNodeResolver = new NodeResolverImpl(mySession);
        myLocalDriverNode = myNodeResolver.fromUUID(node.getHostId()).orElseThrow(IllegalStateException::new);

        ReplicationState replicationState = new ReplicationStateImpl(myNodeResolver, mySession);
        RepairHistoryService repairHistoryService =
                new RepairHistoryService(mySession, replicationState, myNodeResolver, TimeUnit.DAYS.toMillis(30));

        RepairHistoryProvider repairHistoryProvider;
        if (repairHistoryType == RepairHistoryType.ECC)
        {
            repairHistoryProvider = repairHistoryService;
        }
        else if (repairHistoryType == RepairHistoryType.CASSANDRA)
        {
            repairHistoryProvider = new CassandraRepairHistoryService(myNodeResolver, mySession, TimeUnit.DAYS.toMillis(30));
        }
        else
        {
            throw new IllegalArgumentException("Unknown repair history type for test");
        }
        myRepairStatsProvider = new RepairStatsProviderImpl(getNativeConnectionProvider(),
                new VnodeRepairStateFactoryImpl(replicationState, repairHistoryProvider, true));
    }

    @After
    public void clean()
    {
        List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();

        Metadata metadata = mySession.getMetadata();
        for (TableReference tableReference : myRepairs)
        {
            stages.add(mySession.executeAsync(QueryBuilder.deleteFrom("system_distributed", "repair_history")
                    .whereColumn("keyspace_name")
                    .isEqualTo(literal(tableReference.getKeyspace()))
                    .whereColumn("columnfamily_name")
                    .isEqualTo(literal(tableReference.getTable()))
                    .build()
                    .setConsistencyLevel(ConsistencyLevel.ALL)));
            for (Node node : metadata.getNodes().values())
            {
                stages.add(mySession.executeAsync(QueryBuilder.deleteFrom(ECCHRONOS_KEYSPACE, "repair_history")
                        .whereColumn("table_id")
                        .isEqualTo(literal(tableReference.getId()))
                        .whereColumn("node_id")
                        .isEqualTo(literal(node.getHostId()))
                        .build()
                        .setConsistencyLevel(ConsistencyLevel.ALL)));
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
        Node node = getNodeFromDatacenterOne();
        assertThat(node).isNotNull();
        long maxRepairedTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        assertThat(tableReference).isNotNull();
        myRepairs.add(tableReference);
        injectRepairHistory(node, tableReference, maxRepairedTime);
        //Get repairstats for now - 3 hours to now
        long since = maxRepairedTime - TimeUnit.HOURS.toMillis(3);

        RepairStats repairStatsLocal =
                myRepairStatsProvider.getRepairStats(node.getHostId(), tableReference, since, System.currentTimeMillis());
        assertThat(repairStatsLocal).isNotNull();
        assertThat(repairStatsLocal.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsLocal.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsLocal.repairedRatio).isGreaterThan(0.0);
        assertThat(repairStatsLocal.repairTimeTakenMs).isGreaterThan(0);
    }

    @Test
    public void repairInfoForRepairedInSubRanges()
    {
        Node node = getNodeFromDatacenterOne();
        assertThat(node).isNotNull();
        long maxRepairedTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        myRepairs.add(tableReference);
        injectRepairHistory(node, tableReference, maxRepairedTime, true);
        //Get repairstats for now - 3 hours to now
        long since = maxRepairedTime - TimeUnit.HOURS.toMillis(1);

        RepairStats repairStatsLocal = myRepairStatsProvider.getRepairStats(node.getHostId(),
                tableReference, since,
                System.currentTimeMillis());
        assertThat(repairStatsLocal).isNotNull();
        assertThat(repairStatsLocal.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsLocal.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsLocal.repairedRatio).isGreaterThan(0.0);
        assertThat(repairStatsLocal.repairTimeTakenMs).isGreaterThan(0);
    }

    @Test
    public void repairInfoForHalfOfRangesRepaired()
    {
        Node node = getNodeFromDatacenterOne();
        assertThat(node).isNotNull();
        long maxRepairedTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2);
        TableReference tableReference = myTableReferenceFactory.forTable(TEST_KEYSPACE, TEST_TABLE_ONE_NAME);
        myRepairs.add(tableReference);
        Set<TokenRange> expectedRepairedBefore = halfOfTokenRanges(node, tableReference);
        injectRepairHistory(tableReference, maxRepairedTime, expectedRepairedBefore);
        //Get repairstats for now - 3 hours to now
        long since = maxRepairedTime - TimeUnit.HOURS.toMillis(1);
        RepairStats repairStatsLocal = myRepairStatsProvider.getRepairStats(node.getHostId(),
                tableReference,
                since,
                System.currentTimeMillis());
        assertThat(repairStatsLocal).isNotNull();
        assertThat(repairStatsLocal.keyspace).isEqualTo(tableReference.getKeyspace());
        assertThat(repairStatsLocal.table).isEqualTo(tableReference.getTable());
        assertThat(repairStatsLocal.repairedRatio).isGreaterThan(0.0);
        assertThat(repairStatsLocal.repairedRatio).isLessThan(1.0);
    }

    private void injectRepairHistory(Node node, TableReference tableReference, long timestampMax)
    {
        injectRepairHistory(node, tableReference, timestampMax, false);
    }

    private void injectRepairHistory(Node node, TableReference tableReference, long timestampMax, boolean splitRanges)
    {
        Set<TokenRange> tokenRanges = mySession.getMetadata().getTokenMap().get().getTokenRanges(tableReference.getKeyspace(), node);
        injectRepairHistory(tableReference, timestampMax, tokenRanges, splitRanges);
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, Set<TokenRange> tokenRanges)
    {
        injectRepairHistory(tableReference, timestampMax, tokenRanges, false);
    }

    private void injectRepairHistory(TableReference tableReference,
                                     long timestampMax,
                                     Set<TokenRange> tokenRanges,
                                     boolean splitRanges)
    {
        long timestamp = timestampMax - 1;

        for (TokenRange tokenRange : tokenRanges)
        {
            Set<InetAddress> participants = mySession.getMetadata()
                    .getTokenMap()
                    .get()
                    .getReplicas(tableReference.getKeyspace(), tokenRange)
                    .stream()
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

    private void injectRepairHistory(TableReference tableReference,
                                     long timestamp,
                                     Set<InetAddress> participants,
                                     String range_begin,
                                     String range_end)
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
                    .value("status", literal("SUCCESS"))
                    .build();
        }
        else
        {
            Set<UUID> nodes = participants.stream()
                    .map(myNodeResolver::fromIp)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(DriverNode::getId)
                    .collect(Collectors.toSet());

            statement = QueryBuilder.insertInto(ECCHRONOS_KEYSPACE, "repair_history")
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
                    .value("finished_at", literal(Instant.ofEpochMilli(finished_at)))
                    .build();
        }
        mySession.execute(statement.setConsistencyLevel(ConsistencyLevel.ALL));
    }

    private Set<TokenRange> halfOfTokenRanges(Node node, TableReference tableReference)
    {
        Set<TokenRange> halfOfRanges = new HashSet<>();
        Set<TokenRange> allTokenRanges = mySession.getMetadata()
                .getTokenMap()
                .get()
                .getTokenRanges(tableReference.getKeyspace(), node);
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
