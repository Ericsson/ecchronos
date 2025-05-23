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
     * {@inheritDoc}
     */
    @Override
    public VnodeRepairStates calculateNewState(final TableReference tableReference, final RepairStateSnapshot previous,
            final long iterateToTime)
    {
        Map<LongTokenRange, Set<DriverNode>> tokenRangeToReplicaMap
                = myReplicationState.getTokenRangeToReplicas(tableReference);
        long lastRepairedAt = previousLastRepairedAt(previous, tokenRangeToReplicaMap);

        Iterator<RepairEntry> repairEntryIterator;

        if (lastRepairedAt == VnodeRepairState.UNREPAIRED)
        {
            LOG.debug("No last repaired at found for {}, iterating over all repair entries", tableReference);
            repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, iterateToTime,
                    (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        }
        else
        {
            LOG.debug("Table {} snapshot created at {}, iterating repair entries until that time", tableReference,
                    previous.getCreatedAt());
            repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, iterateToTime,
                    previous.getCreatedAt(), (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        }

        return generateVnodeRepairStates(lastRepairedAt, previous, repairEntryIterator, tokenRangeToReplicaMap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VnodeRepairStates calculateState(final TableReference tableReference,
                                            final long to,
                                            final long from)
    {
        Map<LongTokenRange, Set<DriverNode>> tokenRangeToReplicaMap
                = myReplicationState.getTokenRangeToReplicas(tableReference);
        Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, to, from,
                (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        return generateVnodeRepairStates(VnodeRepairState.UNREPAIRED,
                null, repairEntryIterator, tokenRangeToReplicaMap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VnodeRepairStates calculateClusterWideState(final TableReference tableReference,
                                                       final long to,
                                                       final long from)
    {
        Map<LongTokenRange, Set<DriverNode>> tokenRanges = myReplicationState.getTokenRanges(tableReference);
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
                                                        final Map<LongTokenRange, Set<DriverNode>>
                                                                tokenRangeToReplicaMap)
    {
        List<VnodeRepairState> vnodeRepairStatesBase = new ArrayList<>();

        for (Map.Entry<LongTokenRange, Set<DriverNode>> entry : tokenRangeToReplicaMap.entrySet())
        {
            LongTokenRange longTokenRange = entry.getKey();
            Set<DriverNode> replicas = entry.getValue();
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
            Set<DriverNode> replicas = getReplicasForRange(longTokenRange, tokenRangeToReplicaMap);

            VnodeRepairState vnodeRepairState = new VnodeRepairState(longTokenRange,
                    replicas, repairEntry.getStartedAt(), repairEntry.getFinishedAt());

            vnodeRepairStatusesBuilder.updateVnodeRepairState(vnodeRepairState);
        }

        return vnodeRepairStatusesBuilder.build();
    }

    private long previousLastRepairedAt(final RepairStateSnapshot previous,
                                        final Map<LongTokenRange, Set<DriverNode>> tokenToReplicaMap)
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
                                        final Map<LongTokenRange, Set<DriverNode>> tokenRangeToReplicaMap)
    {
        if (RepairStatus.SUCCESS != repairEntry.getStatus())
        {
            LOG.debug("Ignoring entry {}, repair was not successful", repairEntry);
            return false;
        }

        LongTokenRange repairedRange = repairEntry.getRange();

        Set<DriverNode> nodes = getReplicasForRange(repairedRange, tokenRangeToReplicaMap);
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

    private Set<DriverNode> getReplicasForRange(final LongTokenRange range,
                                                         final Map<LongTokenRange, Set<DriverNode>>
                                                                 tokenRangeToReplicaMap)
    {
        Set<DriverNode> nodes = tokenRangeToReplicaMap.get(range);
        if (nodes == null && useSubRanges)
        {
            for (Map.Entry<LongTokenRange, Set<DriverNode>> vnode : tokenRangeToReplicaMap.entrySet())
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
