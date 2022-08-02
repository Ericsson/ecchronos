/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.osgi;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.OptionUtils;
import org.ops4j.pax.exam.cm.ConfigurationAdminOptions;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class ITTableRepairJob extends TestBase
{
    private static final String REPAIR_CONFIGURATION_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.DefaultRepairConfigurationProviderComponent";
    private static final String SCHEDULE_MANAGER_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.ScheduleManagerService";

    private static final String CONFIGURATION_ENABLED = "enabled";
    private static final String CONFIGURATION_SCHEDULE_INTERVAL_IN_SECONDS = "scheduleIntervalInSeconds";

    @Inject
    NativeConnectionProvider myNativeConnectionProvider;

    @Inject
    TableReferenceFactory myTableReferenceFactory;

    @Inject
    RepairScheduler myRepairScheduler;

    @Inject
    NodeResolver myNodeResolver;

    @Inject
    RepairHistoryProvider myRepairHistoryProvider;

    private static CqlSession myAdminSession;

    private Metadata myMetadata;
    private com.datastax.oss.driver.api.core.metadata.Node myLocalHost;
    private Node myLocalNode;

    private Set<TableReference> myRepairs = new HashSet<>();

    private RepairConfiguration myRepairConfiguration;

    @Configuration
    public Option[] configure() throws IOException
    {
        return OptionUtils.combine(basicOptions(), scheduleManagerOptions(), repairSchedulerOptions());
    }

    @BeforeClass
    public static void setupAdminSession()
    {
        myAdminSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(CASSANDRA_HOST, Integer.parseInt(CASSANDRA_NATIVE_PORT)))
                .withAuthCredentials("cassandra", "cassandra")
                .withLocalDatacenter("datacenter1")
                .build();
    }

    @Before
    public void init()
    {
        CqlSession session = myNativeConnectionProvider.getSession();
        myMetadata = session.getMetadata();
        myLocalHost = myNativeConnectionProvider.getLocalNode();

        myLocalNode = myNodeResolver.fromUUID(myLocalHost.getHostId()).orElseThrow(IllegalStateException::new);

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.HOURS)
                .build();
    }

    @After
    public void clean()
    {
        List<CompletionStage<AsyncResultSet>> stages = new ArrayList<>();
        for (TableReference tableReference : myRepairs)
        {
            myRepairScheduler.removeConfiguration(tableReference);

            stages.add(myAdminSession.executeAsync(
                    QueryBuilder.deleteFrom("system_distributed", "repair_history")
                            .whereColumn("keyspace_name").isEqualTo(literal(tableReference.getKeyspace()))
                            .whereColumn("columnfamily_name").isEqualTo(literal(tableReference.getTable()))
                            .build()));
            for (com.datastax.oss.driver.api.core.metadata.Node node : myMetadata.getNodes().values())
            {
                stages.add(myAdminSession.executeAsync(QueryBuilder.deleteFrom("ecchronos", "repair_history")
                        .whereColumn("table_id").isEqualTo(literal(tableReference.getId()))
                        .whereColumn("node_id").isEqualTo(literal(node.getHostId()))
                        .build()));
            }
        }

        for (CompletionStage stage : stages)
        {
            CompletableFutures.getUninterruptibly(stage);
        }
    }

    @AfterClass
    public static void shutdownAdminSession()
    {
        myAdminSession.close();
    }

    /**
     * Create a table that is replicated and was repaired two hours ago.
     *
     * The repair factory should detect the new table automatically and schedule it to run.
     */
    @Test
    public void repairSingleTable()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));

        verifyTableRepairedSince(tableReference, startTime);
    }

    /**
     * Create two tables that are replicated and was repaired two and four hours ago.
     *
     * The repair factory should detect the new tables automatically and schedule them to run.
     */
    @Test
    public void repairMultipleTables()
    {
        long startTime = System.currentTimeMillis();

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");
        TableReference tableReference2 = myTableReferenceFactory.forTable("test", "table2");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
        injectRepairHistory(tableReference2, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4));

        schedule(tableReference);
        schedule(tableReference2);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime));
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference2, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verifyTableRepairedSince(tableReference2, startTime);
    }

    /**
     * Create a table that is replicated and was fully repaired two hours ago.
     *
     * It was also partially repaired by another node.
     *
     * The repair factory should detect the table automatically and schedule it to run on the ranges that were not
     * repaired.
     */
    @Test
    public void partialTableRepair()
    {
        long startTime = System.currentTimeMillis();
        long expectedRepairedInterval = startTime - TimeUnit.HOURS.toMillis(1);

        TableReference tableReference = myTableReferenceFactory.forTable("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        Set<TokenRange> expectedRepairedBefore = halfOfTokenRanges(tableReference);
        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30),
                expectedRepairedBefore);

        Set<TokenRange> allTokenRanges = myMetadata.getTokenMap().get()
                .getTokenRanges(tableReference.getKeyspace(), myLocalHost);
        Set<LongTokenRange> expectedRepairedRanges = Sets.difference(convertTokenRanges(allTokenRanges),
                convertTokenRanges(expectedRepairedBefore));

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS)
                .until(() -> isRepairedSince(tableReference, startTime, expectedRepairedRanges));

        verifyTableRepairedSince(tableReference, expectedRepairedInterval);
    }

    private void schedule(TableReference tableReference)
    {
        if (myRepairs.add(tableReference))
        {
            myRepairScheduler.putConfiguration(tableReference, myRepairConfiguration);
        }
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince,
                tokenRangesFor(tableReference.getKeyspace()));
        assertThat(repairedAt).isPresent();
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince).isPresent();
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired)
    {
        return lastRepairedSince(tableReference, repairedSince, expectedRepaired).isPresent();
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()));
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince,
            Set<LongTokenRange> expectedRepaired)
    {
        Set<LongTokenRange> expectedRepairedCopy = new HashSet<>(expectedRepaired);
        Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(tableReference,
                System.currentTimeMillis(), repairedSince,
                repairEntry -> fullyRepaired(repairEntry) && expectedRepairedCopy.remove(repairEntry.getRange()));

        List<RepairEntry> repairEntries = Lists.newArrayList(repairEntryIterator);

        Set<LongTokenRange> actuallyRepaired = repairEntries.stream()
                .map(RepairEntry::getRange)
                .collect(Collectors.toSet());

        if (expectedRepaired.equals(actuallyRepaired))
        {
            return repairEntries.stream().mapToLong(RepairEntry::getStartedAt).min();
        }
        else
        {
            return OptionalLong.empty();
        }
    }

    private Set<LongTokenRange> tokenRangesFor(String keyspace)
    {
        return myMetadata.getTokenMap().get().getTokenRanges(keyspace, myLocalHost).stream()
                .map(this::convertTokenRange)
                .collect(Collectors.toSet());
    }

    private boolean fullyRepaired(RepairEntry repairEntry)
    {
        return repairEntry.getParticipants().size() == 3 && repairEntry.getStatus() == RepairStatus.SUCCESS;
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax)
    {
        injectRepairHistory(tableReference, timestampMax,
                myMetadata.getTokenMap().get().getTokenRanges(tableReference.getKeyspace(), myLocalHost));
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax, Set<TokenRange> tokenRanges)
    {
        long timestamp = timestampMax - 1;

        for (TokenRange tokenRange : tokenRanges)
        {
            injectRepairHistory(tableReference, timestamp, tokenRange);

            timestamp--;
        }
    }

    private void injectRepairHistory(TableReference tableReference, long timestamp, TokenRange tokenRange)
    {
        long started_at = timestamp;
        long finished_at = timestamp + 5;

        Set<UUID> participants = myMetadata.getTokenMap().get().getReplicas(tableReference.getKeyspace(), tokenRange)
                .stream()
                .map(com.datastax.oss.driver.api.core.metadata.Node::getHostId)
                .collect(Collectors.toSet());

        SimpleStatement statement = QueryBuilder.insertInto("ecchronos", "repair_history")
                .value("table_id", literal(tableReference.getId()))
                .value("node_id", literal(myLocalNode.getId()))
                .value("repair_id", literal(Uuids.startOf(started_at)))
                .value("job_id", literal(tableReference.getId()))
                .value("coordinator_id", literal(myLocalNode.getId()))
                .value("range_begin", literal(Long.toString(((Murmur3Token) tokenRange.getStart()).getValue())))
                .value("range_end", literal(Long.toString(((Murmur3Token) tokenRange.getEnd()).getValue())))
                .value("participants", literal(participants))
                .value("status", literal("SUCCESS"))
                .value("started_at", literal(Instant.ofEpochMilli(started_at)))
                .value("finished_at", literal(Instant.ofEpochMilli(finished_at))).build();

        myAdminSession.execute(statement);
    }

    private Set<TokenRange> halfOfTokenRanges(TableReference tableReference)
    {
        Set<TokenRange> halfOfRanges = new HashSet<>();
        Set<TokenRange> allTokenRanges = myMetadata.getTokenMap().get()
                .getTokenRanges(tableReference.getKeyspace(), myLocalHost);
        Iterator<TokenRange> iterator = allTokenRanges.iterator();
        for (int i = 0; i < allTokenRanges.size() / 2 && iterator.hasNext(); i++)
        {
            halfOfRanges.add(iterator.next());
        }

        return halfOfRanges;
    }

    private Set<LongTokenRange> convertTokenRanges(Set<TokenRange> tokenRanges)
    {
        return tokenRanges.stream().map(this::convertTokenRange).collect(Collectors.toSet());
    }

    private LongTokenRange convertTokenRange(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = ((Murmur3Token) range.getStart()).getValue();
        long end = ((Murmur3Token) range.getEnd()).getValue();
        return new LongTokenRange(start, end);
    }

    private Option repairSchedulerOptions()
    {
        return ConfigurationAdminOptions.newConfiguration(REPAIR_CONFIGURATION_PID)
                .put(CONFIGURATION_ENABLED, false)
                .asOption();
    }

    private Option scheduleManagerOptions()
    {
        return ConfigurationAdminOptions.newConfiguration(SCHEDULE_MANAGER_PID)
                .put(CONFIGURATION_SCHEDULE_INTERVAL_IN_SECONDS, 1L)
                .asOption();
    }
}
