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

import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import org.springframework.http.ResponseEntity;

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
     * @return A list of JSON representations of {@link OnDemandRepair}
     */
    ResponseEntity<List<OnDemandRepair>> getRepairs(String keyspace, String table);
    /**
     * Get a list of on demand repairs associate with a specific id.
     *
     * @param id The id of the on demand repair
     * @return A list of JSON representations of {@link OnDemandRepair}
     */
    ResponseEntity<List<OnDemandRepair>> getRepairs(String id);

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
     * @return A JSON representation of {@link OnDemandRepair}
     */
    ResponseEntity<OnDemandRepair> triggerRepair(String keyspace, String table);

    /**
     * Get a list of the status of all scheduled repair jobs.
     *
     * @return A list of JSON representations of {@link ScheduledRepairJob}
     */
    ResponseEntity<List<ScheduledRepairJob>> status();

    /**
     * Get a list of the status of all scheduled repair jobs for a specific keyspace.
     *
     * @param keyspace The keyspace to list
     * @return A list of JSON representations of {@link ScheduledRepairJob}
     */
    ResponseEntity<List<ScheduledRepairJob>> keyspaceStatus(String keyspace);

    /**
     * Get a list of the status of all scheduled repair jobs for a specific table.
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get status of
     * @return A JSON representation of {@link ScheduledRepairJob}
     */
    ResponseEntity<List<ScheduledRepairJob>> tableStatus(String keyspace, String table);

    /**
     * Get status of a specific scheduled table repair job.
     *
     * @param id The id of the job
     * @return A JSON representation of {@link CompleteRepairJob}
     */
    ResponseEntity<CompleteRepairJob> jobStatus(String id);

    /**
     * Get a list of configuration of all scheduled repair jobs.
     *
     * @return A list of JSON representations of {@link TableRepairConfig}
     */
    ResponseEntity<List<TableRepairConfig>> config();

    /**
     * Get a list of configuration of all scheduled repair jobs for a specific keyspace.
     *
     * @param keyspace The keyspace to list
     * @return A list of JSON representations of {@link TableRepairConfig}
     */
    ResponseEntity<List<TableRepairConfig>> keyspaceConfig(String keyspace);

    /**
     * Get configuration of a specific scheduled table repair job.
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get configuration of
     * @return A JSON representation of {@link TableRepairConfig}
     */
    ResponseEntity<List<TableRepairConfig>> tableConfig(String keyspace, String table);

    /**
     * Get configuration of a specific scheduled table repair job.
     *
     * @param id The id of the table to get configuration of
     * @return A JSON representation of {@link TableRepairConfig}
     */
    ResponseEntity<TableRepairConfig> jobConfig(String id);

    /**
     * Schedule an on demand repair to be run on a specific table
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get configuration of
     * @return A JSON representation of {@link ScheduledRepairJob}
     */
    ResponseEntity<ScheduledRepairJob> scheduleJob(String keyspace, String table);
}
