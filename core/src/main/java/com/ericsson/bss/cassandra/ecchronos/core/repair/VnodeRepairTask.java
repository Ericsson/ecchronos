/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.JmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class VnodeRepairTask extends RepairTask
{
    private static final Logger LOG = LoggerFactory.getLogger(VnodeRepairTask.class);
    private final ConcurrentMap<LongTokenRange, RepairHistory.RepairSession> myRepairSessions =
            new ConcurrentHashMap<>();
    private final Set<LongTokenRange> myTokenRanges;
    private final Set<DriverNode> myReplicas;
    private volatile Set<LongTokenRange> myUnknownRanges;

    public VnodeRepairTask(final JmxProxyFactory jmxProxyFactory, final TableReference tableReference,
            final RepairConfiguration repairConfiguration, final TableRepairMetrics tableRepairMetrics,
            final RepairHistory repairHistory, final Set<LongTokenRange> tokenRanges, final Set<DriverNode> replicas,
            final UUID jobId)
    {
        super(jmxProxyFactory, tableReference, repairConfiguration, tableRepairMetrics);
        myTokenRanges = Preconditions.checkNotNull(tokenRanges, "Token ranges must be set");
        myReplicas = Preconditions.checkNotNull(replicas, "Replicas must be set");
        for (LongTokenRange range : myTokenRanges)
        {
            myRepairSessions.put(range, repairHistory.newSession(tableReference, jobId, range, myReplicas));
        }
    }

    @Override
    protected final void onExecute()
    {
        myRepairSessions.values().forEach(RepairHistory.RepairSession::start);
    }

    @Override
    protected final void onFinish(final RepairStatus repairStatus)
    {
        if (repairStatus.equals(RepairStatus.FAILED))
        {
            Set<LongTokenRange> unrepairedRanges = new HashSet<>();
            if (myUnknownRanges != null)
            {
                unrepairedRanges.addAll(myUnknownRanges);
            }
            unrepairedRanges.addAll(getFailedRanges());
            LOG.warn("Unable to repair '{}', affected ranges: '{}'", this, unrepairedRanges);
        }
        myRepairSessions.values().forEach(rs -> rs.finish(repairStatus));
        myRepairSessions.clear();
    }

    @Override
    protected final void verifyRepair(final JmxProxy proxy) throws ScheduledJobException
    {
        Set<LongTokenRange> completedRanges = Sets.union(getFailedRanges(), getSuccessfulRanges());
        Set<LongTokenRange> unknownRanges = Sets.difference(myTokenRanges, completedRanges);
        if (!unknownRanges.isEmpty())
        {
            LOG.debug("Unknown ranges: {}", unknownRanges);
            LOG.debug("Completed ranges: {}", completedRanges);
            myUnknownRanges = Collections.unmodifiableSet(unknownRanges);
            proxy.forceTerminateAllRepairSessions();
            throw new ScheduledJobException(String.format("Unknown status of some ranges for %s", this));
        }
        super.verifyRepair(proxy);
    }

    @Override
    protected final Map<String, String> getOptions()
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOptions.PARALLELISM_KEY, getRepairConfiguration().getRepairParallelism().getName());
        options.put(RepairOptions.PRIMARY_RANGE_KEY, Boolean.toString(false));
        options.put(RepairOptions.COLUMNFAMILIES_KEY, getTableReference().getTable());
        options.put(RepairOptions.INCREMENTAL_KEY, Boolean.toString(false));

        StringBuilder rangesStringBuilder = new StringBuilder();
        for (LongTokenRange range : myTokenRanges)
        {
            rangesStringBuilder.append(range.start).append(':').append(range.end).append(',');
        }
        options.put(RepairOptions.RANGES_KEY, rangesStringBuilder.toString());
        String replicasString = myReplicas.stream().map(host -> host.getPublicAddress().getHostAddress())
                .collect(Collectors.joining(","));
        options.put(RepairOptions.HOSTS_KEY, replicasString);
        return options;
    }

    @Override
    protected final void onRangeFinished(final LongTokenRange range, final RepairStatus repairStatus)
    {
        super.onRangeFinished(range, repairStatus);
        RepairHistory.RepairSession repairSession = myRepairSessions.remove(range);
        if (repairSession == null)
        {
            LOG.error("{}: Finished range {} - but not included in the known repair sessions {}, all ranges are {}",
                    this,
                    range,
                    myRepairSessions.keySet(),
                    myTokenRanges);
        }
        else
        {
            repairSession.finish(repairStatus);
        }
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("Vnode repairTask of %s", getTableReference());
    }

    @VisibleForTesting
    final Collection<LongTokenRange> getUnknownRanges()
    {
        return myUnknownRanges;
    }

    @VisibleForTesting
    final Set<LongTokenRange> getTokenRanges()
    {
        return Sets.newLinkedHashSet(myTokenRanges);
    }

    @VisibleForTesting
    final Set<DriverNode> getReplicas()
    {
        return Sets.newHashSet(myReplicas);
    }
}
