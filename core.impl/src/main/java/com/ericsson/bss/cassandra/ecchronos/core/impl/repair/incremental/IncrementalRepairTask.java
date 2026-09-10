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
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairTask;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
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

    private final RepairHistory.RepairSession myRepairSession;

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
            final Set<DriverNode> participants)
    {
        super(currentNode, jmxProxyFactory, tableReference, repairConfiguration, tableRepairMetrics,
                jmxProxyFactory.getMaxWaitTimeInMinutes());
        myRepairSession = repairHistory.newSession(historyNode, tableReference, jobId, FULL_RANGE, participants,
                repairConfiguration.getRepairType());
    }

    @Override
    protected final void onExecute()
    {
        myRepairSession.start();
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
