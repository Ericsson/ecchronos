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
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IncrementalRepairGroupFactory implements ReplicaRepairGroupFactory
{
    private final TableReference myTableReference;
    private final ReplicationState myReplicationState;

    public IncrementalRepairGroupFactory(TableReference tableReference, ReplicationState replicationState)
    {
        myTableReference = tableReference;
        myReplicationState = replicationState;
    }

    @Override
    public List<ReplicaRepairGroup> generateReplicaRepairGroups(List<VnodeRepairState> availableVnodeRepairStates)
    {
        if (availableVnodeRepairStates.isEmpty())
        {
            return Collections.emptyList();
        }

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicaMap = myReplicationState.getTokenRangeToReplicas(myTableReference);

        Set<LongTokenRange> repairableRanges = availableVnodeRepairStates.stream().map(VnodeRepairState::getTokenRange).collect(Collectors.toSet());

        if (Sets.symmetricDifference(tokenRangeToReplicaMap.keySet(), repairableRanges).isEmpty())
        {
            Set<Host> allHosts = tokenRangeToReplicaMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet());

            ReplicaRepairGroup replicaRepairGroup = new ReplicaRepairGroup(allHosts, Lists.newArrayList(repairableRanges));

            return Collections.singletonList(replicaRepairGroup);
        }

        return Collections.emptyList();
    }
}
