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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metrics.CassandraMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class used to run Incremental Repairs in Cassandra.
 * <p>
 * When constructed with a {@link RepairHistory} (and the required node/job/participants) the task records a repair
 * session in {@code ecchronos.repair_history}, keyed by the coordinator node. When constructed without a repair
 * history (or with {@link RepairHistory#NO_OP}) it behaves as before and only logs, preserving backward compatibility.
 */
public class IncrementalRepairTask extends RepairTask
{
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalRepairTask.class);

    /** An incremental repair covers the whole owned token space; we record it against the full range. */
    private static final LongTokenRange FULL_RANGE = LongTokenRange.of(Long.MIN_VALUE, Long.MAX_VALUE);

    private static final double PERCENT_FULLY_REPAIRED = 100.0d;

    /** Sentinel values returned by {@link CassandraMetrics} when a metric cannot be fetched. */
    private static final long METRIC_UNAVAILABLE = 0L;
    private static final double PERCENT_UNAVAILABLE = 0.0d;

    private final RepairHistory.RepairSession myRepairSession;

    // Optional Layer A confirmation (issue #1812): when set, after a repair reports success the task confirms
    // that the repaired state actually advanced. Left null preserves the previous behavior.
    private final CassandraMetrics myCassandraMetrics;
    private final UUID myMetricsNodeId;

    // Pre-repair snapshot captured in onExecute() and compared against a fresh reading in verifyRepair().
    private volatile double myPrePercentRepaired = PERCENT_FULLY_REPAIRED;
    private volatile long myPreMaxRepairedAt = Long.MAX_VALUE;

    /**
     * Constructs an IncrementalRepairTask for a specific node and table without repair-history tracking.
     *
     * @param currentNode the UUID of the current node where the repair task is running. Must not be {@code null}.
     * @param jmxProxyFactory the factory to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param tableReference the reference to the table that is being repaired. Must not be {@code null}.
     * @param repairConfiguration the configuration specifying how the repair task should be executed. Must not be {@code null}.
     * @param tableRepairMetrics the metrics associated with table repairs for monitoring and tracking purposes. Must not be {@code null}.
     */
    public IncrementalRepairTask(
            final UUID currentNode,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final TableRepairMetrics tableRepairMetrics)
    {
        super(currentNode, jmxProxyFactory, tableReference, repairConfiguration, tableRepairMetrics,
                jmxProxyFactory.getMaxWaitTimeInMinutes());
        myRepairSession = RepairHistory.NO_OP.newSession(null, tableReference, UUID.randomUUID(), FULL_RANGE,
                Set.of(), repairConfiguration.getRepairType());
        myCassandraMetrics = null;
        myMetricsNodeId = null;
    }

    /**
     * Constructs an IncrementalRepairTask that records its execution in {@code ecchronos.repair_history}.
     *
     * @param currentNode the UUID of the current node where the repair task is running. Must not be {@code null}.
     * @param jmxProxyFactory the factory to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param tableReference the reference to the table that is being repaired. Must not be {@code null}.
     * @param repairConfiguration the configuration specifying how the repair task should be executed. Must not be {@code null}.
     * @param tableRepairMetrics the metrics associated with table repairs for monitoring and tracking purposes. Must not be {@code null}.
     * @param repairHistory the repair history used to record the session. Must not be {@code null}.
     * @param historyNode the node used as the {@code node_id} for the repair history session. Must not be {@code null}.
     * @param jobId the identifier of the owning repair job. Must not be {@code null}.
     * @param participants the participants recorded for the repair session. Must not be {@code null}.
     * @param cassandraMetrics metrics source used to confirm the repair advanced repaired state (issue #1812),
     *                         or {@code null} to skip the confirmation.
     */
    public IncrementalRepairTask(
            final UUID currentNode,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final TableRepairMetrics tableRepairMetrics,
            final RepairHistory repairHistory,
            final Node historyNode,
            final UUID jobId,
            final Set<DriverNode> participants,
            final CassandraMetrics cassandraMetrics)
    {
        super(currentNode, jmxProxyFactory, tableReference, repairConfiguration, tableRepairMetrics,
                jmxProxyFactory.getMaxWaitTimeInMinutes());
        myRepairSession = repairHistory.newSession(historyNode, tableReference, jobId, FULL_RANGE, participants,
                repairConfiguration.getRepairType());
        myCassandraMetrics = cassandraMetrics;
        myMetricsNodeId = currentNode;
    }

    @Override
    protected final void onExecute()
    {
        if (myCassandraMetrics != null && myMetricsNodeId != null)
        {
            // Snapshot the repaired state before the repair so verifyRepair() can confirm it advanced.
            // Force a fresh reading so the "before" value is ground truth measured on the same basis as the
            // post-repair reading; a stale cached "before" could otherwise be lower than the actual state at
            // repair start and mask a repair that did no work.
            myCassandraMetrics.forceRefresh(myMetricsNodeId, getTableReference());
            myPrePercentRepaired = myCassandraMetrics.getPercentRepaired(myMetricsNodeId, getTableReference());
            myPreMaxRepairedAt = myCassandraMetrics.getMaxRepairedAt(myMetricsNodeId, getTableReference());
            LOG.debug("{} - pre-repair state percentRepaired={}, maxRepairedAt={}",
                    this, myPrePercentRepaired, myPreMaxRepairedAt);
        }
        myRepairSession.start();
    }

    /**
     * In addition to the base failed-range check, confirm (issue #1812) that a repair which otherwise looks
     * successful actually advanced the repaired state. If the table had pending/unrepaired data before the repair
     * but neither {@code maxRepairedAt} advanced nor {@code percentRepaired} increased afterwards, the session did
     * no work (for example an incremental prepare-phase abort) and must not be reported as a success.
     *
     * @param proxy the JMX proxy.
     * @throws ScheduledJobException if the repair had failed ranges or did not advance the repaired state.
     */
    @Override
    protected final void verifyRepair(final DistributedJmxProxy proxy) throws ScheduledJobException
    {
        super.verifyRepair(proxy);

        if (myCassandraMetrics == null || myMetricsNodeId == null)
        {
            return;
        }

        boolean hadPendingData = myPrePercentRepaired < PERCENT_FULLY_REPAIRED;
        if (!hadPendingData)
        {
            // Nothing to repair (steady state, fully repaired) — repaired_at legitimately does not advance.
            return;
        }

        // Force a fresh reading so we do not compare against a stale cached value.
        myCassandraMetrics.forceRefresh(myMetricsNodeId, getTableReference());
        long postMaxRepairedAt = myCassandraMetrics.getMaxRepairedAt(myMetricsNodeId, getTableReference());
        double postPercentRepaired = myCassandraMetrics.getPercentRepaired(myMetricsNodeId, getTableReference());

        // CassandraMetrics returns sentinel 0/0.0 when it cannot fetch the metric (e.g. a transient JMX error).
        // In that case we cannot confirm either way; do not turn a metrics hiccup into a false repair failure.
        boolean postReadingUnavailable = postMaxRepairedAt == METRIC_UNAVAILABLE
                && postPercentRepaired == PERCENT_UNAVAILABLE;
        if (postReadingUnavailable)
        {
            LOG.warn("{} - unable to read post-repair metrics for {}; skipping repaired-state confirmation",
                    this, getTableReference());
            return;
        }

        boolean repairedStateAdvanced =
                postMaxRepairedAt > myPreMaxRepairedAt || postPercentRepaired > myPrePercentRepaired;
        if (!repairedStateAdvanced)
        {
            // Do not force-terminate here: unlike the failed/unknown-range checks (where a session ran and
            // misbehaved), "did not advance" is an inference that no work happened and does not imply our session
            // is still running. forceTerminateAllRepairSessions() is cluster-wide and would also abort other
            // tables' legitimately-running incremental repairs, so we only fail the task.
            throw new ScheduledJobException(String.format(
                    "Incremental repair of %s reported success but repaired state did not advance "
                            + "(percentRepaired %.2f -> %.2f, maxRepairedAt %d -> %d); no data was repaired",
                    getTableReference(), myPrePercentRepaired, postPercentRepaired,
                    myPreMaxRepairedAt, postMaxRepairedAt));
        }
        LOG.debug("{} - confirmed repaired state advanced (percentRepaired {} -> {}, maxRepairedAt {} -> {})",
                this, myPrePercentRepaired, postPercentRepaired, myPreMaxRepairedAt, postMaxRepairedAt);
    }

    @Override
    protected final Map<String, String> getOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOptions.PARALLELISM_KEY, getRepairConfiguration().getRepairParallelism().getName());
        options.put(RepairOptions.PRIMARY_RANGE_KEY, Boolean.toString(false));
        options.put(RepairOptions.COLUMNFAMILIES_KEY, getTableReference().getTable());
        options.put(RepairOptions.INCREMENTAL_KEY, Boolean.toString(true));
        options.put(RepairOptions.UNREPLICATED_KEY, Boolean.toString(true));

        return options;
    }

    @Override
    protected final void onFinish(final RepairStatus repairStatus)
    {
        if (repairStatus.equals(RepairStatus.FAILED))
        {
            LOG.warn("Unable to repair '{}', affected ranges: '{}'", this, getFailedRanges());
        }
        myRepairSession.finish(repairStatus);
    }

    @Override
    protected final void onRangeFinished(final LongTokenRange range, final RepairStatus repairStatus)
    {
        super.onRangeFinished(range, repairStatus);
        LOG.debug("{} for range {}", repairStatus, range);
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Incremental repairTask of %s", getTableReference());
    }
}
