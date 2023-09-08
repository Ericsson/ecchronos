/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.core.utils;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStateUtils;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;

import java.util.Collection;
import java.util.stream.Collectors;

public class RepairStatsProviderImpl implements RepairStatsProvider
{
    private final VnodeRepairStateFactory myVnodeRepairStateFactory;

    public RepairStatsProviderImpl(final VnodeRepairStateFactory vnodeRepairStateFactory)
    {
        myVnodeRepairStateFactory = vnodeRepairStateFactory;
    }

    @Override
    public final RepairStats getRepairStats(final TableReference tableReference,
                                            final long since,
                                            final long to,
                                            final boolean isLocal)
    {
        VnodeRepairStates vnodeRepairStates;
        if (isLocal)
        {
            vnodeRepairStates = myVnodeRepairStateFactory.calculateState(tableReference, to, since);
        }
        else
        {
            vnodeRepairStates = myVnodeRepairStateFactory.calculateClusterWideState(tableReference, to, since);
        }
        Collection<VnodeRepairState> states = vnodeRepairStates.getVnodeRepairStates();
        Collection<VnodeRepairState> repairedStates = states.stream().filter(s -> isRepaired(s, since, to)).collect(
                Collectors.toList());
        double repairedRatio = states.isEmpty() ? 0 : (double) repairedStates.size() / states.size();
        return new RepairStats(tableReference.getKeyspace(), tableReference.getTable(), repairedRatio,
                VnodeRepairStateUtils.getRepairTime(repairedStates));
    }

    private boolean isRepaired(final VnodeRepairState state, final long since, final long to)
    {
        return state.getStartedAt() >= since && state.getFinishedAt() <= to;
    }
}
