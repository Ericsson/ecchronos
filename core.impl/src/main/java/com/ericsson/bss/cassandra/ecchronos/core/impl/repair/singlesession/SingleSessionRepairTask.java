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
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a full repair of a node in one repair session, with the ranges worked out by Cassandra from the node the
 * repair is started on. It is used for two repair types:
 * <ul>
 *     <li>{@link RepairType#PRIMARY_RANGE}: all primary ranges of the node, like {@code nodetool repair -pr}.</li>
 *     <li>{@link RepairType#ALL_REPLICATED_RANGES}: every range the node replicates, with the replicas in all
 *     datacenters, like {@code nodetool repair} without {@code -pr}. The only difference to the other type is the
 *     {@code primaryRange} option.</li>
 * </ul>
 * The repair is a full (not incremental) repair that is not limited to token ranges or hosts: Cassandra rejects a
 * primary range repair combined with {@code ranges} or {@code hosts}, and the other type repairs everything the
 * node replicates unless it is told otherwise.
 * <p>
 * The task records one repair session in {@code ecchronos.repair_history}, keyed by the coordinator node and typed
 * with the repair type of the task. The row covers the whole token space only as a marker: it does not say which
 * ranges were repaired, so the row must never be used as vnode repair state. The vnode repair state skips rows of
 * both types.
 */
public class SingleSessionRepairTask extends RepairTask
{
    private static final Logger LOG = LoggerFactory.getLogger(SingleSessionRepairTask.class);

    /** Marker range stored in the history row. It does not describe what was repaired. */
    private static final LongTokenRange FULL_RANGE_MARKER = LongTokenRange.of(Long.MIN_VALUE, Long.MAX_VALUE);

    private final RepairHistory.RepairSession myRepairSession;
    private final RepairType myRepairType;

    /**
     * Constructs a SingleSessionRepairTask that records its execution in {@code ecchronos.repair_history}.
     *
     * @param repairType the repair type, {@link RepairType#PRIMARY_RANGE} or {@link RepairType#ALL_REPLICATED_RANGES}.
     * @param currentNode the id of the node the repair is started on. Must not be {@code null}.
     * @param jmxProxyFactory the factory to create connections to distributed JMX proxies. Must not be {@code null}.
     * @param tableReference the table to repair. Must not be {@code null}.
     * @param repairConfiguration the repair configuration. Must not be {@code null}.
     * @param tableRepairMetrics the table repair metrics.
     * @param repairHistory the repair history used to record the session. Must not be {@code null}.
     * @param historyNode the node used as {@code node_id} of the history session. Must not be {@code null}.
     * @param jobId the id of the owning repair job. Must not be {@code null}.
     * @param participants the participants recorded for the session. Must contain {@code historyNode}.
     */
    public SingleSessionRepairTask(
            final RepairType repairType,
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
        Preconditions.checkNotNull(repairHistory, "Repair history must be set");
        myRepairType = Preconditions.checkNotNull(repairType, "Repair type must be set");
        Preconditions.checkArgument(
                repairType == RepairType.PRIMARY_RANGE || repairType == RepairType.ALL_REPLICATED_RANGES,
                "Not a repair type of one session per node: %s", repairType);
        // The type is given by the job and not taken from the configuration, so the history row is always typed as
        // the repair that was run and is recognised as such by the vnode repair state.
        myRepairSession = repairHistory.newSession(historyNode, tableReference, jobId, FULL_RANGE_MARKER,
                participants, repairType);
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
        options.put(RepairOptions.PRIMARY_RANGE_KEY, Boolean.toString(myRepairType == RepairType.PRIMARY_RANGE));
        options.put(RepairOptions.COLUMNFAMILIES_KEY, getTableReference().getTable());
        options.put(RepairOptions.INCREMENTAL_KEY, Boolean.toString(false));
        options.put(RepairOptions.UNREPLICATED_KEY, Boolean.toString(true));
        // No RANGES_KEY and no HOSTS_KEY: Cassandra rejects them together with primaryRange, and without it the
        // ranges are the ones the node replicates.
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

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("%s repairTask of %s", myRepairType, getTableReference());
    }
}
