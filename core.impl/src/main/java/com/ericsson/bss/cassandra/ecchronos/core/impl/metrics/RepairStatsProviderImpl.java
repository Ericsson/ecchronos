/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.metrics;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStateUtils;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

public class RepairStatsProviderImpl implements RepairStatsProvider
{
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final VnodeRepairStateFactory myVnodeRepairStateFactory;

    public RepairStatsProviderImpl(
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final VnodeRepairStateFactory vnodeRepairStateFactory)
    {
        myVnodeRepairStateFactory = vnodeRepairStateFactory;
        myNativeConnectionProvider = nativeConnectionProvider;
    }

    @Override
    public final RepairStats getRepairStats(
            final UUID nodeID,
            final TableReference tableReference,
            final long since,
            final long to)
    {
        Node node = myNativeConnectionProvider.getNodes().get(nodeID);
        VnodeRepairStates vnodeRepairStates;
        vnodeRepairStates = myVnodeRepairStateFactory.calculateClusterWideState(node, tableReference, to, since);
        Collection<VnodeRepairState> states = vnodeRepairStates.getVnodeRepairStates();
        Collection<VnodeRepairState> repairedStates = states.stream()
                .filter(s -> isRepaired(s, since, to))
                .collect(Collectors.toList());
        double repairedRatio = states.isEmpty() ? 0 : (double) repairedStates.size() / states.size();
        return new RepairStats(tableReference.getKeyspace(), tableReference.getTable(), repairedRatio,
                VnodeRepairStateUtils.getRepairTime(repairedStates));
    }

    private boolean isRepaired(final VnodeRepairState state, final long since, final long to)
    {
        return state.getStartedAt() >= since && state.getFinishedAt() <= to;
    }
}

