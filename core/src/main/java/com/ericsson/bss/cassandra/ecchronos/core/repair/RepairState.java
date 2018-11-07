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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;

/**
 * Interface used by ScheduledRepairJob to maintain the current repair state.
 */
public interface RepairState
{
    /**
     * Check if a repair can be performed based on the current state.
     *
     * @return True if repair can run.
     */
    boolean canRepair();

    /**
     * Get the time of the last successful repair of the table.
     *
     * @return The time the table was last repaired or -1 if no information is available.
     */
    long lastRepairedAt();

    /**
     * Get the replicas that should be part of the repair.
     *
     * This method will return an empty Set if all hosts should be part of the repair.
     *
     * @return The set of replicas that should be part of the repair.
     */
    Set<Host> getReplicas();

    /**
     * Get the ranges that needs to be repaired for the table.
     *
     * @return The ranges that should be repaired.
     */
    Collection<LongTokenRange> getLocalRangesForRepair();

    /**
     * Get all the local ranges for the node.
     *
     * @return The local ranges for the node.
     */
    Collection<LongTokenRange> getAllRanges();

    /**
     * Get a map of the ranges and hosts associated to those ranges that needs to be repaired.
     *
     * @return The ranges in combination with the hosts to repair.
     */
    Map<LongTokenRange, Collection<Host>> getRangeToReplicas();

    /**
     * Update the repair state for the table.
     */
    void update();

    /**
     * Get a collection of all data centers that should be part of the repair.
     *
     * @return The collection of data centers.
     */
    Collection<String> getDatacentersForRepair();
}
