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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A repair state factory which uses a {@link RepairHistoryProvider} to determine repair state.
 */
public class VnodeRepairStateFactoryImpl implements VnodeRepairStateFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(VnodeRepairStateFactoryImpl.class);

    private final ReplicationState myReplicationState;
    private final RepairHistoryProvider myRepairHistoryProvider;
    private final boolean useSubRanges;

    public VnodeRepairStateFactoryImpl(final ReplicationState replicationState,
                                       final RepairHistoryProvider repairHistoryProvider,
                                       final boolean toUseSubRanges)
    {
        myReplicationState = replicationState;
        myRepairHistoryProvider = repairHistoryProvider;
        this.useSubRanges = toUseSubRanges;
    }

    /**
     * Calculate new state.
     *
     * @param tableReference The table to calculate the new repair state for vnodes.
     * @param previous The previous repair state or null if non exists.
     * @return VnodeRepairStates
     */
    @Override
    public VnodeRepairStates calculateNewState(final TableReference tableReference, final RepairStateSnapshot previous)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap
                = myReplicationState.getTokenRangeToReplicas(tableReference);
        long lastRepairedAt = previousLastRepairedAt(previous, tokenRangeToReplicaMap);
        long now = System.currentTimeMillis();

        Iterator<RepairEntry> repairEntryIterator;

        if (lastRepairedAt == VnodeRepairState.UNREPAIRED)
        {
            LOG.debug("No last repaired at found for {}, iterating over all repair entries", tableReference);
            repairEntryIterator = myRepairHistoryProvider.iterate(tableReference,
                    now, (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        }
        else
        {
            LOG.debug("Table {} last repaired at {}, iterating repair entries until that time",
                    tableReference, lastRepairedAt);
            repairEntryIterator = myRepairHistoryProvider.iterate(tableReference,
                    now, lastRepairedAt, (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        }

        return generateVnodeRepairStates(lastRepairedAt, previous, repairEntryIterator, tokenRangeToReplicaMap);
    }

    /**
     * Calculate state.
     *
     * @param tableReference The table to calculate the repair state for vnodes.
     * @param to Timestamp from when the repair state should start
     * @param from Timestamp to when the repair state should stop
     * @return VnodeRepairStates
     */
    @Override
    public VnodeRepairStates calculateState(final TableReference tableReference,
                                            final long to,
                                            final long from)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap
                = myReplicationState.getTokenRangeToReplicas(tableReference);
        Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, to, from,
                (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        return generateVnodeRepairStates(VnodeRepairState.UNREPAIRED,
                null, repairEntryIterator, tokenRangeToReplicaMap);
    }

    /**
     * Calculate cluster wide state.
     *
     * @param tableReference The table to calculate the repair state for vnodes.
     * @param to Timestamp from when the repair state should start
     * @param from Timestamp to when the repair state should stop
     * @return VnodeRepairStates
     */
    @Override
    public VnodeRepairStates calculateClusterWideState(final TableReference tableReference,
                                                       final long to,
                                                       final long from)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRanges = myReplicationState.getTokenRanges(tableReference);
        Set<DriverNode> allNodes = new HashSet<>();
        tokenRanges.values().forEach(n -> allNodes.addAll(n));
        List<RepairEntry> allRepairEntries = new ArrayList<>();
        for (DriverNode node : allNodes)
        {
            Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(node.getId(),
                    tableReference, to, from, (repairEntry) -> acceptRepairEntries(repairEntry, tokenRanges));
            while (repairEntryIterator.hasNext())
            {
                RepairEntry repairEntry = repairEntryIterator.next();
                allRepairEntries.add(repairEntry);
            }
        }
        return generateVnodeRepairStates(VnodeRepairState.UNREPAIRED, null, allRepairEntries.iterator(), tokenRanges);
    }

    private VnodeRepairStates generateVnodeRepairStates(final long lastRepairedAt,
                                                        final RepairStateSnapshot previous,
                                                        final Iterator<RepairEntry> repairEntryIterator,
                                                        final Map<LongTokenRange, ImmutableSet<DriverNode>>
                                                                tokenRangeToReplicaMap)
    {
        List<VnodeRepairState> vnodeRepairStatesBase = new ArrayList<>();

        for (Map.Entry<LongTokenRange, ImmutableSet<DriverNode>> entry : tokenRangeToReplicaMap.entrySet())
        {
            LongTokenRange longTokenRange = entry.getKey();
            ImmutableSet<DriverNode> replicas = entry.getValue();
            vnodeRepairStatesBase.add(new VnodeRepairState(longTokenRange, replicas, lastRepairedAt));
        }

        VnodeRepairStates.Builder vnodeRepairStatusesBuilder;
        if (useSubRanges)
        {
            vnodeRepairStatusesBuilder = SubRangeRepairStates.newBuilder(vnodeRepairStatesBase);
        }
        else
        {
            vnodeRepairStatusesBuilder = VnodeRepairStatesImpl.newBuilder(vnodeRepairStatesBase);
        }

        if (previous != null)
        {
            vnodeRepairStatusesBuilder.updateVnodeRepairStates(previous.getVnodeRepairStates().getVnodeRepairStates());
        }

        while (repairEntryIterator.hasNext())
        {
            RepairEntry repairEntry = repairEntryIterator.next();
            LongTokenRange longTokenRange = repairEntry.getRange();
            ImmutableSet<DriverNode> replicas = getReplicasForRange(longTokenRange, tokenRangeToReplicaMap);

            VnodeRepairState vnodeRepairState = new VnodeRepairState(longTokenRange,
                    replicas, repairEntry.getStartedAt(), repairEntry.getFinishedAt());

            vnodeRepairStatusesBuilder.updateVnodeRepairState(vnodeRepairState);
        }

        return vnodeRepairStatusesBuilder.build();
    }

    private long previousLastRepairedAt(final RepairStateSnapshot previous,
                                        final Map<LongTokenRange, ImmutableSet<DriverNode>> tokenToReplicaMap)
    {
        if (previous == null)
        {
            return VnodeRepairState.UNREPAIRED;
        }

        long defaultUsedLastRepairedAt = previous.lastCompletedAt();

        long lastRepairedAt = Long.MAX_VALUE;

        for (VnodeRepairState vnodeRepairState : previous.getVnodeRepairStates().getVnodeRepairStates())
        {
            if (tokenToReplicaMap.containsKey(vnodeRepairState.getTokenRange())
                    && lastRepairedAt > vnodeRepairState.lastRepairedAt())
            {
                lastRepairedAt = vnodeRepairState.lastRepairedAt();
            }
        }

        if (lastRepairedAt == VnodeRepairState.UNREPAIRED)
        {
            return defaultUsedLastRepairedAt;
        }

        return lastRepairedAt == Long.MAX_VALUE ? VnodeRepairState.UNREPAIRED : lastRepairedAt;
    }

    private boolean acceptRepairEntries(final RepairEntry repairEntry,
                                        final Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap)
    {
        if (RepairStatus.SUCCESS != repairEntry.getStatus())
        {
            LOG.debug("Ignoring entry {}, repair was not successful", repairEntry);
            return false;
        }

        LongTokenRange repairedRange = repairEntry.getRange();

        ImmutableSet<DriverNode> nodes = getReplicasForRange(repairedRange, tokenRangeToReplicaMap);
        if (nodes == null)
        {
            LOG.trace("Ignoring entry {}, replicas not present in tokenRangeToReplicas", repairEntry);
            return false;
        }

        if (!nodes.equals(repairEntry.getParticipants()))
        {
            LOG.debug("Ignoring entry {}, replicas {} not matching participants", repairEntry, nodes);
            return false;
        }

        return true;
    }

    private ImmutableSet<DriverNode> getReplicasForRange(final LongTokenRange range,
                                                         final Map<LongTokenRange, ImmutableSet<DriverNode>>
                                                                 tokenRangeToReplicaMap)
    {
        ImmutableSet<DriverNode> nodes = tokenRangeToReplicaMap.get(range);
        if (nodes == null && useSubRanges)
        {
            for (Map.Entry<LongTokenRange, ImmutableSet<DriverNode>> vnode : tokenRangeToReplicaMap.entrySet())
            {
                if (vnode.getKey().isCovering(range))
                {
                    nodes = vnode.getValue();
                    break;
                }
            }
        }

        return nodes;
    }
}
