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

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class VnodeRepairGroupFactory implements ReplicaRepairGroupFactory
{
    public static final VnodeRepairGroupFactory INSTANCE = new VnodeRepairGroupFactory();

    private VnodeRepairGroupFactory()
    {
        // Nothing to do here
    }

    @Override
    public ReplicaRepairGroup generateReplicaRepairGroup(List<VnodeRepairState> availableVnodeRepairStatess)
    {
        Optional<VnodeRepairState> optionalVnodeRepairState = availableVnodeRepairStatess.stream()
                .min(Comparator.comparingLong(VnodeRepairState::lastRepairedAt));

        if (!optionalVnodeRepairState.isPresent())
        {
            return null;
        }

        VnodeRepairState vnodeRepairState = optionalVnodeRepairState.get();
        Set<Host> replicas = vnodeRepairState.getReplicas();

        List<LongTokenRange> commonVnodes = availableVnodeRepairStatess.stream()
                .filter(v -> v.getReplicas().equals(replicas))
                .map(VnodeRepairState::getTokenRange)
                .collect(Collectors.toList());

        return new ReplicaRepairGroup(replicas, commonVnodes);
    }
}
