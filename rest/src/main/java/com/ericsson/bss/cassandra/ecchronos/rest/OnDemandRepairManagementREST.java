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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.util.UUID;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * On Demand Repair REST interface.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface OnDemandRepairManagementREST
{
    /**
     * Get a list of on demand repairs. Will fetch all if no keyspace or table is specified.
     *
     * @param keyspace The keyspace of the table (optional)
     * @param table The table to get status of (optional)
     * @param hostId The hostId of the on demand repair (optional)
     * @return A list of JSON representations of {@link OnDemandRepair}
     */
    ResponseEntity<List<OnDemandRepair>> getRepairs(String keyspace, String table, String hostId);
    /**
     * Get a list of on demand repairs associated with a specific id.
     *
     * @param id The id of the on demand repair
     * @param hostId The hostId of the on demand repair (optional)
     * @return A list of JSON representations of {@link OnDemandRepair}
     */
    ResponseEntity<List<OnDemandRepair>> getRepairs(String id, String hostId);

    /**
     * Schedule an on demand repair to be run on a specific table.
     *
     * @param nodeID The node to execute repair
     * @param keyspace The keyspace of the table
     * @param table The table
     * @param repairType The type of repair (optional)
     * @param isLocal If repair should be only run for the local node (optional)
     * @return A JSON representation of {@link OnDemandRepair}
     */
    ResponseEntity<List<OnDemandRepair>> runRepair(
            UUID nodeID, String keyspace, String table, RepairType repairType,
            boolean isLocal);
}

