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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

/**
 * Reproduction of the cross-node incremental deduplication coverage gap.
 * <p>
 * Context (verified against {@code IncrementalRepairJob} and {@code RepairHistoryService}):
 * <ul>
 *     <li>An incremental repair is scoped to the ranges the coordinator node <em>owns</em>
 *         ({@code primaryRange=false} includes the node's secondary replicas, but still only the ranges that
 *         node stores) — it is NOT cluster-wide coverage from a single node.</li>
 *     <li>On success a node writes one {@code ecchronos.repair_history} row <em>per replica</em> of the ranges
 *         it owns, and every row is stamped with the FULL_RANGE marker ({@code Long.MIN..Long.MAX}), discarding
 *         which concrete ranges were actually repaired.</li>
 *     <li>{@code refreshState()} reads history keyed by the node's own id; if it finds a recent SUCCESS row it
 *         advances {@code lastSuccessfulRun} and the node skips its own run for the interval.</li>
 * </ul>
 * The gap: node B can be marked "already repaired" purely because a peer (node A) wrote a FULL_RANGE SUCCESS
 * row keyed to B — even though A only repaired the ranges A owns (the A/B shared ranges), leaving B's
 * <em>exclusive</em> ranges unrepaired. B's own ground-truth {@code percentRepaired} is still &lt; 100%, yet the
 * history-based skip wins. This test pins that behavior: the job becomes not-runnable while real unrepaired
 * data remains.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestIncrementalRepairCrossNodeDedupGap
{
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";
    private static final long RUN_INTERVAL_IN_DAYS = 1;
    private static final long WARNING_IN_DAYS = 7;
    private static final long ERROR_IN_DAYS = 10;

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private ReplicationState myReplicationState;

    @Mock
    private CassandraMetrics myCassandraMetrics;

    @Mock
    private Node myNodeB;

    @Mock
    private RepairHistoryProvider myRepairHistoryProvider;

    @Mock
    private RepairHistory myRepairHistory;

    private final TableReference myTableReference = tableReference(KEYSPACE_NAME, TABLE_NAME);
    private final UUID myNodeBId = UUID.randomUUID();
    private RepairConfiguration myRepairConfiguration;

    @Before
    public void startup()
    {
        doReturn(myNodeBId).when(myNodeB).getHostId();
        myRepairConfiguration = RepairConfiguration.newBuilder()
                .withParallelism(RepairParallelism.PARALLEL)
                .withRepairWarningTime(WARNING_IN_DAYS, TimeUnit.DAYS)
                .withRepairErrorTime(ERROR_IN_DAYS, TimeUnit.DAYS)
                .withRepairInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .withRepairType(RepairType.INCREMENTAL)
                .build();
    }

    /**
     * Demonstrates the bug. Node B still has unrepaired data ({@code percentRepaired = 50%}), but a peer
     * (node A) has written a FULL_RANGE SUCCESS row keyed to B for the shared ranges. After {@code refreshState()}
     * node B wrongly considers itself done and becomes not-runnable, so B's exclusive ranges are never repaired
     * this interval.
     * <p>
     * This assertion encodes the CURRENT (buggy) behavior so the reproduction is unambiguous. When the dedup is
     * fixed (e.g. per-node-strict history, or range-aware coverage), B must remain runnable while
     * {@code percentRepaired < 100%}; at that point flip the expectations marked below.
     */
    @Test
    public void nodeBSkipsItsIntervalBecauseAPeerWroteAFullRangeSuccessRow()
    {
        // Ground truth from Cassandra: node B is only 50% repaired — it genuinely has data left to repair.
        doReturn(50.0d).when(myCassandraMetrics).getPercentRepaired(myNodeBId, myTableReference);
        // At construction there is no history yet, so the job starts runnable.
        doReturn(Collections.emptyIterator())
                .when(myRepairHistoryProvider).iterate(eq(myNodeB), eq(myTableReference), anyLong(), anyLong(), any());

        IncrementalRepairJob jobB = buildJobForNodeB();
        assertThat(jobB.runnable()).as("Node B must start runnable: it is only 50%% repaired").isTrue();

        // Peer node A repairs the A/B shared ranges and, per recordCompletedRepair, writes a FULL_RANGE SUCCESS
        // row for every replica it is responsible for — including one keyed to node B. B's exclusive ranges
        // (which A does not own and therefore did not repair) are untouched.
        long peerCompletedAt = System.currentTimeMillis();
        doReturn(fullRangeSuccessWrittenByPeer(peerCompletedAt))
                .when(myRepairHistoryProvider).iterate(eq(myNodeB), eq(myTableReference), anyLong(), anyLong(), any());

        jobB.refreshState();

        // BUG: despite still being 50% repaired, node B now treats the peer's FULL_RANGE row as its own
        // completion and will not run this interval -> B's exclusive ranges are left unrepaired.
        // --- Expectations below encode the current buggy behavior. Flip them once the dedup is range-aware. ---
        assertThat(jobB.getLastSuccessfulRun())
                .as("current (buggy) behavior: B adopts the peer's completion timestamp")
                .isGreaterThanOrEqualTo(peerCompletedAt);
        assertThat(jobB.runnable())
                .as("current (buggy) behavior: B is skipped even though percentRepaired is only 50%%")
                .isFalse();
    }

    /**
     * Control case: with NO peer history row, node B at 50% repaired stays runnable. This isolates the cause —
     * the skip in the test above is driven solely by the cross-node FULL_RANGE history row, not by the metric.
     */
    @Test
    public void nodeBStaysRunnableWithoutPeerHistoryRow()
    {
        doReturn(50.0d).when(myCassandraMetrics).getPercentRepaired(myNodeBId, myTableReference);
        doReturn(Collections.emptyIterator())
                .when(myRepairHistoryProvider).iterate(eq(myNodeB), eq(myTableReference), anyLong(), anyLong(), any());

        IncrementalRepairJob jobB = buildJobForNodeB();
        jobB.refreshState();

        assertThat(jobB.runnable())
                .as("with no peer row, a half-repaired node must run")
                .isTrue();
    }

    /**
     * One row with the FULL_RANGE marker — exactly what {@code RepairHistoryService.recordCompletedRepair}
     * writes for node B when peer node A completes (range stamped Long.MIN..Long.MAX, status SUCCESS).
     */
    private Iterator<RepairEntry> fullRangeSuccessWrittenByPeer(final long finishedAt)
    {
        RepairEntry entry = new RepairEntry(
                LongTokenRange.of(Long.MIN_VALUE, Long.MAX_VALUE),
                finishedAt - 1000,
                finishedAt,
                Collections.emptySet(),
                RepairStatus.SUCCESS.name());
        return Arrays.asList(entry).iterator();
    }

    private IncrementalRepairJob buildJobForNodeB()
    {
        ScheduledJob.Configuration configuration = new ScheduledJob.ConfigurationBuilder()
                .withPriority(ScheduledJob.Priority.LOW)
                .withRunInterval(RUN_INTERVAL_IN_DAYS, TimeUnit.DAYS)
                .build();

        return new IncrementalRepairJob.Builder()
                .withConfiguration(configuration)
                .withTableReference(myTableReference)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withReplicationState(myReplicationState)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withRepairConfiguration(myRepairConfiguration)
                .withCassandraMetrics(myCassandraMetrics)
                .withNode(myNodeB)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .build();
    }
}
