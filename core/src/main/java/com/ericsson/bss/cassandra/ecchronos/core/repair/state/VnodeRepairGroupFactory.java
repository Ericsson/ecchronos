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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A factory for {@link ReplicaRepairGroup} that creates a repair group for all vnodes with common replicas.
 *
 * The generated {@link ReplicaRepairGroup} will contain the vnode which is most urgent to repair and all other vnodes
 * which have common replicas.
 */
public final class VnodeRepairGroupFactory implements ReplicaRepairGroupFactory
{
    public static final VnodeRepairGroupFactory INSTANCE = new VnodeRepairGroupFactory();

    private VnodeRepairGroupFactory()
    {
        // Nothing to do here
    }

    @Override
    public List<ReplicaRepairGroup> generateReplicaRepairGroups(List<VnodeRepairState> availableVnodeRepairStates)
    {
        List<VnodeRepairState> sortedVnodeRepairStates = availableVnodeRepairStates.stream()
                .sorted(Comparator.comparingLong(VnodeRepairState::lastRepairedAt))
                .collect(Collectors.toList());

        List<ReplicaRepairGroup> sortedRepairGroups = new ArrayList<>();
        Set<Set<Host>> countedReplicaGroups = new HashSet<>();

        for (VnodeRepairState vnodeRepairState : sortedVnodeRepairStates)
        {
            Set<Host> replicas = vnodeRepairState.getReplicas();

            if (countedReplicaGroups.add(replicas))
            {
                List<LongTokenRange> commonVnodes = availableVnodeRepairStates.stream()
                        .filter(v -> v.getReplicas().equals(replicas))
                        .map(VnodeRepairState::getTokenRange)
                        .collect(Collectors.toList());

                sortedRepairGroups.add(new ReplicaRepairGroup(replicas, commonVnodes));
            }
        }

        return sortedRepairGroups;
    }
}
