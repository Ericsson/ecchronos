/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairInfo;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.util.List;

/**
 * Repair scheduler rest interface.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface RepairManagementREST
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
     * Get a list of on demand repairs associate with a specific id.
     *
     * @param id The id of the on demand repair
     * @param hostId The hostId of the on demand repair (optional)
     * @return A list of JSON representations of {@link OnDemandRepair}
     */
    ResponseEntity<List<OnDemandRepair>> getRepairs(String id, String hostId);

    /**
     * Get a list of schedules. Will fetch all if no keyspace or table is specified.
     *
     * @param keyspace The keyspace of the table (optional)
     * @param table The table to get status of (optional)
     * @return A list of JSON representations of {@link Schedule}
     */
    ResponseEntity<List<Schedule>> getSchedules(String keyspace, String table);

    /**
     * Get schedule with a specific id.
     *
     * @param id The id of the schedule
     * @param full Whether to include token range information.
     * @return A JSON representation of {@link Schedule}
     */
    ResponseEntity<Schedule> getSchedules(String id, boolean full);

    /**
     * Schedule an on demand repair to be run on a specific table.
     *
     * @param keyspace The keyspace of the table
     * @param table The table
     * @param isLocal If repair should be only run for the local node (optional)
     * @return A JSON representation of {@link OnDemandRepair}
     */
    ResponseEntity<List<OnDemandRepair>> triggerRepair(String keyspace, String table, boolean isLocal);

    ResponseEntity<RepairInfo> getRepairInfo(String keyspace, String table, Long since, Duration duration);
}
