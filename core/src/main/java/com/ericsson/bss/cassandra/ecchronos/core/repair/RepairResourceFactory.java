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

import java.util.Set;

/**
 * Interface for generating repair resources to lock when running repair.
 */
@FunctionalInterface
public interface RepairResourceFactory
{
    /**
     * Generate repair resources to lock based on the provided {@link ReplicaRepairGroup}.
     *
     * It is up to the implementation to decide which repair resources needs to be locked for the repair group.
     *
     * @param replicaRepairGroup The replica repair group.
     * @return The repair resources that needs to be locked for the repair to run.
     */
    Set<RepairResource> getRepairResources(ReplicaRepairGroup replicaRepairGroup);
}
