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

import java.util.List;

/**
 * An factory interface that creates {@link ReplicaRepairGroup ReplicaRepairGroups} based on the currently repairable
 * vnodes.
 */
@FunctionalInterface
public interface ReplicaRepairGroupFactory
{
    /**
     * Generate a sorted list of {@link ReplicaRepairGroup} based on the provided {@link VnodeRepairState}.
     *
     * It is assumed that all vnodes passed to this method should be repaired (now).
     *
     * Which vnodes/replicas are included in the {@link ReplicaRepairGroup} is up to the specific implementation but
     * the list should be sorted with the most urgent {@link ReplicaRepairGroup} first.
     *
     * @param availableVnodeRepairStates The currently repairable vnodes.
     * @return The repair groups based on the provided vnodes.
     */
    List<ReplicaRepairGroup> generateReplicaRepairGroups(List<VnodeRepairState> availableVnodeRepairStates);
}
