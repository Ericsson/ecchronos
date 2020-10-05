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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.junit.*;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.OptionUtils;
import org.ops4j.pax.exam.cm.ConfigurationAdminOptions;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistoryProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
    RepairScheduler myRepairScheduler;

    private static Cluster myAdminCluster;
    private static Session myAdminSession;
    private Metadata myMetadata;
    private Host myLocalHost;

    private RepairHistoryProvider myRepairHistoryProvider;

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
        myAdminCluster = Cluster.builder().addContactPoint(CASSANDRA_HOST)
                .withPort(Integer.valueOf(CASSANDRA_NATIVE_PORT))
                .withCredentials("cassandra", "cassandra")
                .build();
        myAdminSession = myAdminCluster.connect();
    }

    @Before
    public void init()
    {
        Session session = myNativeConnectionProvider.getSession();
        myMetadata = session.getCluster().getMetadata();
        myLocalHost = myNativeConnectionProvider.getLocalHost();

        myRepairHistoryProvider = new RepairHistoryProviderImpl(session, s -> s, TimeUnit.DAYS.toMillis(30));

        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.HOURS)
                .build();
    }

    @After
    public void clean()
    {
        for (TableReference tableReference : myRepairs)
        {
            myRepairScheduler.removeConfiguration(tableReference);

            myAdminSession.execute(QueryBuilder.delete()
                    .from("system_distributed", "repair_history")
                    .where(QueryBuilder.eq("keyspace_name", tableReference.getKeyspace()))
                    .and(QueryBuilder.eq("columnfamily_name", tableReference.getTable())));
        }
    }

    @AfterClass
    public static void shutdownAdminSession()
    {
        myAdminSession.close();
        myAdminCluster.close();
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

        TableReference tableReference = new TableReference("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime));

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

        TableReference tableReference = new TableReference("test", "table1");
        TableReference tableReference2 = new TableReference("test", "table2");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));
        injectRepairHistory(tableReference2, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(4));

        schedule(tableReference);
        schedule(tableReference2);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime));
        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference2, startTime));

        verifyTableRepairedSince(tableReference, startTime);
        verifyTableRepairedSince(tableReference2, startTime);
    }

    /**
     * Create a table that is replicated and was fully repaired two hours ago.
     *
     * It was also partially repaired by another node.
     *
     * The repair factory should detect the table automatically and schedule it to run on the ranges that were not repaired.
     */
    @Test
    public void partialTableRepair()
    {
        long startTime = System.currentTimeMillis();
        long expectedRepairedInterval = startTime - TimeUnit.HOURS.toMillis(1);

        TableReference tableReference = new TableReference("test", "table1");

        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

        Set<TokenRange> expectedRepairedBefore = halfOfTokenRanges(tableReference);
        injectRepairHistory(tableReference, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(30), expectedRepairedBefore);

        Set<TokenRange> allTokenRanges = myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost);
        Set<LongTokenRange> expectedRepairedRanges = Sets.difference(convertTokenRanges(allTokenRanges), convertTokenRanges(expectedRepairedBefore));

        schedule(tableReference);

        await().pollInterval(1, TimeUnit.SECONDS).atMost(90, TimeUnit.SECONDS).until(() -> isRepairedSince(tableReference, startTime, expectedRepairedRanges));

        verifyTableRepairedSince(tableReference, expectedRepairedInterval, expectedRepairedRanges);
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
        verifyTableRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()));
    }

    private void verifyTableRepairedSince(TableReference tableReference, long repairedSince, Set<LongTokenRange> expectedRepaired)
    {
        OptionalLong repairedAt = lastRepairedSince(tableReference, repairedSince);
        assertThat(repairedAt).isPresent();
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince).isPresent();
    }

    private boolean isRepairedSince(TableReference tableReference, long repairedSince, Set<LongTokenRange> expectedRepaired)
    {
        return lastRepairedSince(tableReference, repairedSince, expectedRepaired).isPresent();
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince)
    {
        return lastRepairedSince(tableReference, repairedSince, tokenRangesFor(tableReference.getKeyspace()));
    }

    private OptionalLong lastRepairedSince(TableReference tableReference, long repairedSince, Set<LongTokenRange> expectedRepaired)
    {
        Set<LongTokenRange> expectedRepairedCopy = new HashSet<>(expectedRepaired);
        Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, System.currentTimeMillis(), repairedSince, repairEntry -> fullyRepaired(repairEntry) && expectedRepairedCopy.remove(repairEntry.getRange()));

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
        return myMetadata.getTokenRanges(keyspace, myLocalHost).stream()
                .map(this::convertTokenRange)
                .collect(Collectors.toSet());
    }

    private boolean fullyRepaired(RepairEntry repairEntry)
    {
        return repairEntry.getParticipants().size() == 3 && repairEntry.getStatus() == RepairStatus.SUCCESS;
    }

    private void injectRepairHistory(TableReference tableReference, long timestampMax)
    {
        injectRepairHistory(tableReference, timestampMax, myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost));
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

        Set<InetAddress> participants = myMetadata.getReplicas(tableReference.getKeyspace(), tokenRange).stream().map(Host::getAddress).collect(Collectors.toSet());

        Insert insert = QueryBuilder.insertInto("system_distributed", "repair_history")
                .value("keyspace_name", tableReference.getKeyspace())
                .value("columnfamily_name", tableReference.getTable())
                .value("participants", participants)
                .value("id", UUIDs.startOf(started_at))
                .value("started_at", new Date(started_at))
                .value("finished_at", new Date(finished_at))
                .value("range_begin", tokenRange.getStart().toString())
                .value("range_end", tokenRange.getEnd().toString())
                .value("status", "SUCCESS");

        myAdminSession.execute(insert);
    }

    private Set<TokenRange> halfOfTokenRanges(TableReference tableReference)
    {
        Set<TokenRange> halfOfRanges = new HashSet<>();
        Set<TokenRange> allTokenRanges = myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost);
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
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
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
