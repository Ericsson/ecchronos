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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

/**
 * Repair resource factory that creates a combination of repair resources based on other implementations.
 */
public class CombinedRepairResourceFactory implements RepairResourceFactory
{
    private final ImmutableSet<RepairResourceFactory> myRepairResourceFactories;

    public CombinedRepairResourceFactory(RepairResourceFactory... repairResourceFactories)
    {
        myRepairResourceFactories = ImmutableSet.copyOf(repairResourceFactories);
    }

    @Override
    public Set<RepairResource> getRepairResources(ReplicaRepairGroup replicaRepairGroup)
    {
        Set<RepairResource> repairResources = new HashSet<>();

        for (RepairResourceFactory repairResourceFactory : myRepairResourceFactories)
        {
            repairResources.addAll(repairResourceFactory.getRepairResources(replicaRepairGroup));
        }

        return repairResources;
    }
}
