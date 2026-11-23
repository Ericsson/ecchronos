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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.primaryrange;

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
 * Runs a full repair of all primary ranges of a node in one repair session, like {@code nodetool repair -pr}.
 * <p>
 * The repair is a full (not incremental) repair that is not limited to token ranges or hosts: Cassandra rejects a
 * primary range repair combined with {@code ranges} or {@code hosts}, and works out the ranges from the node the
 * repair is started on.
 * <p>
 * The task records one repair session in {@code ecchronos.repair_history}, keyed by the coordinator node and typed
 * {@link RepairType#PRIMARY_RANGE}. The row covers the whole token space only as a marker: a primary range repair
 * does not repair the replicas' other ranges, so the row must never be used as vnode repair state. The vnode repair
 * state skips rows of this type.
 */
public class PrimaryRangeRepairTask extends RepairTask
{
    private static final Logger LOG = LoggerFactory.getLogger(PrimaryRangeRepairTask.class);

    /** Marker range stored in the history row. It does not describe what was repaired. */
    private static final LongTokenRange FULL_RANGE_MARKER = LongTokenRange.of(Long.MIN_VALUE, Long.MAX_VALUE);

    private final RepairHistory.RepairSession myRepairSession;

    /**
     * Constructs a PrimaryRangeRepairTask that records its execution in {@code ecchronos.repair_history}.
     *
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
    public PrimaryRangeRepairTask(
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
        // The type is fixed here and not taken from the configuration, so the history row is always typed as a
        // primary range row and is recognised as such by the vnode repair state.
        myRepairSession = repairHistory.newSession(historyNode, tableReference, jobId, FULL_RANGE_MARKER,
                participants, RepairType.PRIMARY_RANGE);
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
        options.put(RepairOptions.PRIMARY_RANGE_KEY, Boolean.toString(true));
        options.put(RepairOptions.COLUMNFAMILIES_KEY, getTableReference().getTable());
        options.put(RepairOptions.INCREMENTAL_KEY, Boolean.toString(false));
        options.put(RepairOptions.UNREPLICATED_KEY, Boolean.toString(true));
        // No RANGES_KEY and no HOSTS_KEY: Cassandra rejects them together with primaryRange.
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
        return String.format("Primary range repairTask of %s", getTableReference());
    }
}
