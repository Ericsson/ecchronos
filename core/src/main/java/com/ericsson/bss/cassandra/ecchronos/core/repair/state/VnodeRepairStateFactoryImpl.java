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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A repair state factory which uses a {@link RepairHistoryProvider} to determine repair state.
 */
public class VnodeRepairStateFactoryImpl implements VnodeRepairStateFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(VnodeRepairStateFactoryImpl.class);

    private final ReplicationState myReplicationState;
    private final RepairHistoryProvider myRepairHistoryProvider;
    private final boolean useSubRanges;

    public VnodeRepairStateFactoryImpl(ReplicationState replicationState, RepairHistoryProvider repairHistoryProvider, boolean useSubRanges)
    {
        myReplicationState = replicationState;
        myRepairHistoryProvider = repairHistoryProvider;
        this.useSubRanges = useSubRanges;
    }

    @Override
    public VnodeRepairStates calculateNewState(TableReference tableReference, RepairStateSnapshot previous)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap = myReplicationState.getTokenRangeToReplicas(tableReference);
        long lastRepairedAt = previousLastRepairedAt(previous, tokenRangeToReplicaMap);
        long now = System.currentTimeMillis();

        Iterator<RepairEntry> repairEntryIterator;

        if (lastRepairedAt == VnodeRepairState.UNREPAIRED)
        {
            LOG.debug("No last repaired at found for {}, iterating over all repair entries", tableReference);
            repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, now, (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        }
        else
        {
            LOG.debug("Table {} last repaired at {}, iterating repair entries until that time", tableReference, lastRepairedAt);
            repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, now, lastRepairedAt, (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        }

        return generateVnodeRepairStates(lastRepairedAt, previous, repairEntryIterator, tokenRangeToReplicaMap);
    }

    @Override
    public VnodeRepairStates calculateState(TableReference tableReference, long to, long from)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap = myReplicationState.getTokenRangeToReplicas(tableReference);
        Iterator<RepairEntry> repairEntryIterator = myRepairHistoryProvider.iterate(tableReference, to, from,
                (repairEntry) -> acceptRepairEntries(repairEntry, tokenRangeToReplicaMap));
        return generateVnodeRepairStates(VnodeRepairState.UNREPAIRED, null, repairEntryIterator, tokenRangeToReplicaMap);
    }

    private VnodeRepairStates generateVnodeRepairStates(long lastRepairedAt, RepairStateSnapshot previous, Iterator<RepairEntry> repairEntryIterator, Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap)
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

        while(repairEntryIterator.hasNext())
        {
            RepairEntry repairEntry = repairEntryIterator.next();
            LongTokenRange longTokenRange = repairEntry.getRange();
            ImmutableSet<DriverNode> replicas = getReplicasForRange(longTokenRange, tokenRangeToReplicaMap);

            VnodeRepairState vnodeRepairState = new VnodeRepairState(longTokenRange, replicas, repairEntry.getStartedAt(), repairEntry.getFinishedAt());

            vnodeRepairStatusesBuilder.updateVnodeRepairState(vnodeRepairState);
        }

        return vnodeRepairStatusesBuilder.build();
    }

    private long previousLastRepairedAt(RepairStateSnapshot previous, Map<LongTokenRange, ImmutableSet<DriverNode>> tokenToReplicaMap)
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

    private boolean acceptRepairEntries(RepairEntry repairEntry, Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap)
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

    private ImmutableSet<DriverNode> getReplicasForRange(LongTokenRange range, Map<LongTokenRange, ImmutableSet<DriverNode>> tokenRangeToReplicaMap)
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
