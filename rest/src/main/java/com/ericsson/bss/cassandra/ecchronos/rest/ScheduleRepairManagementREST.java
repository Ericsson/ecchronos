/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * Schedule REST interface.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface ScheduleRepairManagementREST
{
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
}
