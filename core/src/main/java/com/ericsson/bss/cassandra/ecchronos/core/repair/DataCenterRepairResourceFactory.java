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

import java.util.Set;
import java.util.stream.Collectors;

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicaRepairGroup;

/**
 * Repair resource factory that generates one repair resource per data center involved in the repair.
 */
public class DataCenterRepairResourceFactory implements RepairResourceFactory
{
    @Override
    public Set<RepairResource> getRepairResources(ReplicaRepairGroup replicaRepairGroup)
    {
        return replicaRepairGroup.getDataCenters().stream()
            .map(this::dataCenterToRepairResource)
            .collect(Collectors.toSet());
    }

    private RepairResource dataCenterToRepairResource(String dataCenter)
    {
        return new RepairResource(dataCenter, dataCenter);
    }
}
