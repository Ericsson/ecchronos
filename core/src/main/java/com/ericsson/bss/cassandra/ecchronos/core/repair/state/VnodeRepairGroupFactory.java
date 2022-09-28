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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A factory for {@link ReplicaRepairGroup} that creates repair groups for all vnodes with common replicas.
 *
 * The generated list will contain the vnode groups in a sorted order so that the most urgent vnode to
 * repair is first in the list.
 */
public final class VnodeRepairGroupFactory implements ReplicaRepairGroupFactory
{
    public static final VnodeRepairGroupFactory INSTANCE = new VnodeRepairGroupFactory();

    private VnodeRepairGroupFactory()
    {
        // Nothing to do here
    }

    @Override
    public List<ReplicaRepairGroup> generateReplicaRepairGroups(final List<VnodeRepairState> availableVnodeRepairStates)
    {
        List<VnodeRepairState> sortedVnodeRepairStates = availableVnodeRepairStates.stream()
                .sorted(Comparator.comparingLong(VnodeRepairState::lastRepairedAt))
                .collect(Collectors.toList());

        List<ReplicaRepairGroup> sortedRepairGroups = new ArrayList<>();
        Set<Set<DriverNode>> countedReplicaGroups = new HashSet<>();

        for (VnodeRepairState vnodeRepairState : sortedVnodeRepairStates)
        {
            ImmutableSet<DriverNode> replicas = vnodeRepairState.getReplicas();

            if (countedReplicaGroups.add(replicas))
            {
                List<LongTokenRange> commonVnodes = availableVnodeRepairStates.stream()
                        .filter(v -> v.getReplicas().equals(replicas))
                        .map(VnodeRepairState::getTokenRange)
                        .collect(Collectors.toList());

                sortedRepairGroups.add(new ReplicaRepairGroup(replicas, ImmutableList.copyOf(commonVnodes)));
            }
        }

        return sortedRepairGroups;
    }
}
