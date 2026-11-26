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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.singlesession;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestReplicatedRangesCoverage
{
    private static final LongTokenRange RANGE_1 = LongTokenRange.of(0, 10);
    private static final LongTokenRange RANGE_2 = LongTokenRange.of(10, 20);
    private static final LongTokenRange RANGE_3 = LongTokenRange.of(20, 30);

    private final TableReference myTable = tableReference("keyspace", "table");
    private final TableReference myOtherTable = tableReference("keyspace", "other");
    private final TableReference myOtherKeyspace = tableReference("other_keyspace", "table");

    private final ListAppender<ILoggingEvent> myLogAppender = new ListAppender<>();
    private final Logger myLogger = (Logger) LoggerFactory.getLogger(ReplicatedRangesCoverage.class);

    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private Clock myClock;
    @Mock
    private Node myNodeA;
    @Mock
    private Node myNodeB;
    @Mock
    private Node myNodeC;

    private final UUID myIdA = UUID.randomUUID();
    private final UUID myIdB = UUID.randomUUID();
    private final UUID myIdC = UUID.randomUUID();
    private final List<Node> myManaged = new ArrayList<>();
    private long myNow = 1_000_000L;

    private ReplicatedRangesCoverage myCoverage;

    @Before
    public void setup()
    {
        myLogAppender.start();
        myLogger.addAppender(myLogAppender);
        when(myNodeA.getHostId()).thenReturn(myIdA);
        when(myNodeB.getHostId()).thenReturn(myIdB);
        when(myNodeC.getHostId()).thenReturn(myIdC);
        when(myClock.millis()).thenAnswer(invocation -> myNow);
        myManaged.add(myNodeA);
        myCoverage = new ReplicatedRangesCoverage(myReplicationState, () -> (Collection<Node>) myManaged, myClock);
    }

    @After
    public void teardown()
    {
        myLogger.detachAppender(myLogAppender);
        myLogAppender.stop();
    }

    private DriverNode replica(final UUID id)
    {
        DriverNode node = mock(DriverNode.class);
        when(node.getId()).thenReturn(id);
        return node;
    }

    /** Ranges 1 and 2 are replicated by nodes A and B, range 3 by nodes B and C. */
    private void ring()
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> ranges = ImmutableMap.of(
                RANGE_1, ImmutableSet.of(replica(myIdA), replica(myIdB)),
                RANGE_2, ImmutableSet.of(replica(myIdA), replica(myIdB)),
                RANGE_3, ImmutableSet.of(replica(myIdB), replica(myIdC)));
        when(myReplicationState.getTokenRanges(any(), any())).thenReturn(ranges);
    }

    private List<ILoggingEvent> warnings()
    {
        return myLogAppender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
    }

    @Test
    public void testNothingIsUncoveredWhenTheManagedNodesReplicateEveryRange()
    {
        ring();
        myManaged.add(myNodeC);

        assertThat(myCoverage.uncoveredRanges(myNodeA, myTable)).isEmpty();
    }

    @Test
    public void testARangeNoManagedNodeReplicatesIsUncovered()
    {
        ring();
        // Only node A is managed. Range 3 is replicated by B and C.
        assertThat(myCoverage.uncoveredRanges(myNodeA, myTable)).containsExactly(RANGE_3);
    }

    @Test
    public void testOneManagedReplicaIsEnoughToCoverARange()
    {
        ring();
        myManaged.clear();
        myManaged.add(myNodeC);

        // C replicates range 3 only, so ranges 1 and 2 are not reached.
        assertThat(myCoverage.uncoveredRanges(myNodeC, myTable)).containsExactlyInAnyOrder(RANGE_1, RANGE_2);
    }

    @Test
    public void testWithoutManagedNodesEveryRangeIsUncovered()
    {
        ring();
        myManaged.clear();

        assertThat(myCoverage.uncoveredRanges(myNodeA, myTable)).containsExactlyInAnyOrder(RANGE_1, RANGE_2, RANGE_3);
    }

    @Test
    public void testManagedNodesAreReadEveryTime()
    {
        ring();
        assertThat(myCoverage.uncoveredRanges(myNodeA, myTable)).hasSize(1);

        myManaged.add(myNodeB);

        assertThat(myCoverage.uncoveredRanges(myNodeA, myTable)).isEmpty();
    }

    @Test
    public void testAWarningNamesTheKeyspaceAndTheNumberOfRanges()
    {
        ring();

        myCoverage.warnIfUncovered(myNodeA, myTable);

        assertThat(warnings()).hasSize(1);
        String message = warnings().get(0).getFormattedMessage();
        assertThat(message).contains("keyspace").contains("1 of its token ranges").contains("ALL_REPLICATED_RANGES");
    }

    @Test
    public void testNoWarningWhenEveryRangeIsReached()
    {
        ring();
        myManaged.add(myNodeC);

        myCoverage.warnIfUncovered(myNodeA, myTable);

        assertThat(warnings()).isEmpty();
    }

    @Test
    public void testTheWarningIsGivenOncePerKeyspaceAndNotPerTable()
    {
        ring();

        myCoverage.warnIfUncovered(myNodeA, myTable);
        myCoverage.warnIfUncovered(myNodeA, myOtherTable);
        myCoverage.warnIfUncovered(myNodeB, myTable);

        assertThat(warnings()).hasSize(1);
    }

    @Test
    public void testEveryKeyspaceWithAGapIsWarnedAbout()
    {
        ring();

        myCoverage.warnIfUncovered(myNodeA, myTable);
        myCoverage.warnIfUncovered(myNodeA, myOtherKeyspace);

        assertThat(warnings()).hasSize(2);
        assertThat(warnings().get(1).getFormattedMessage()).contains("other_keyspace");
    }

    @Test
    public void testTheWarningIsRepeatedAfterTheInterval()
    {
        ring();

        myCoverage.warnIfUncovered(myNodeA, myTable);
        myNow += ReplicatedRangesCoverage.REWARN_INTERVAL_MS - 1;
        myCoverage.warnIfUncovered(myNodeA, myTable);
        assertThat(warnings()).hasSize(1);

        myNow += 1;
        myCoverage.warnIfUncovered(myNodeA, myTable);
        assertThat(warnings()).hasSize(2);
    }

    @Test
    public void testAChangedGapIsWarnedAboutAtOnce()
    {
        ring();
        myCoverage.warnIfUncovered(myNodeA, myTable);

        // Node B is no longer managed either, so the gap grows.
        myManaged.clear();
        myManaged.add(myNodeC);
        myCoverage.warnIfUncovered(myNodeC, myTable);

        assertThat(warnings()).hasSize(2);
    }

    @Test
    public void testAGapThatWasClosedAndOpensAgainIsWarnedAboutAtOnce()
    {
        ring();
        myCoverage.warnIfUncovered(myNodeA, myTable);
        myManaged.add(myNodeC);
        myCoverage.warnIfUncovered(myNodeA, myTable);
        myManaged.remove(myNodeC);

        myCoverage.warnIfUncovered(myNodeA, myTable);

        assertThat(warnings()).hasSize(2);
    }

    @Test
    public void testAFailureToReadTheTokenMapDoesNotFailTheCaller()
    {
        when(myReplicationState.getTokenRanges(any(), any())).thenThrow(new IllegalStateException("no token map"));

        myCoverage.warnIfUncovered(myNodeA, myTable);

        assertThat(warnings()).isEmpty();
    }
}
