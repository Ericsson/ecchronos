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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.parseIdOrThrow;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * When updating the path it should also be updated in the OSGi component.
 */
@Tag(name = "Repair-Management", description = "Management of repairs")
@RestController
public class ScheduleRepairManagementRESTImpl implements ScheduleRepairManagementREST
{
    @Autowired
    private final RepairScheduler myRepairScheduler;

    public ScheduleRepairManagementRESTImpl(final RepairScheduler repairScheduler)
    {
        myRepairScheduler = repairScheduler;
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/running-job", produces = MediaType.APPLICATION_JSON_VALUE)
    public final ResponseEntity<String> getCurrentJobStatus()
    {
        return ResponseEntity.ok(myRepairScheduler.getCurrentJobStatus());
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/schedules", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-schedules", description = "Get schedules", summary = "Get schedules")
    public final ResponseEntity<List<Schedule>> getSchedules(
            @RequestParam(required = false)
            @Parameter(description = "Filter schedules based on this keyspace, mandatory if 'table' is provided.")
            final String keyspace,
            @RequestParam(required = false)
            @Parameter(description = "Filter schedules based on this table.")
            final String table)
    {
        return ResponseEntity.ok(getListOfSchedules(keyspace, table));
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/schedules/{id}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-schedules-by-id", description = "Get schedules matching the id.",
            summary = "Get schedules matching the id.")
    public final ResponseEntity<Schedule> getSchedules(
            @PathVariable
            @Parameter(description = "The id of the schedule.")
            final String id,
            @RequestParam(required = false)
            @Parameter(description = "Decides if a 'full schedule' should be returned.")
            final boolean full)
    {
        return ResponseEntity.ok(getScheduleView(id, full));

    }

    private List<Schedule> getScheduledRepairJobs(final Predicate<ScheduledRepairJobView> filter)
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(filter)
                .map(Schedule::new)
                .collect(Collectors.toList());
    }

    private Schedule getScheduleView(final String id, final boolean full)
    {
        UUID uuid = parseIdOrThrow(id);
        Optional<ScheduledRepairJobView> view = myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(job -> job.getId().equals(uuid)).findFirst();
        if (!view.isPresent())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return new Schedule(view.get(), full);
    }

    private List<Schedule> getListOfSchedules(final String keyspace, final String table)
    {
        if (keyspace != null)
        {
            if (table != null)
            {
                return getScheduledRepairJobs(forTableSchedule(keyspace, table));
            }
            return getScheduledRepairJobs(
                    job -> keyspace.equals(job.getTableReference().getKeyspace()));
        }
        else if (table == null)
        {
            return getScheduledRepairJobs(job -> true);
        }
        throw new ResponseStatusException(BAD_REQUEST);
    }

    private static Predicate<ScheduledRepairJobView> forTableSchedule(final String keyspace, final String table)
    {
        return tableView ->
        {
            TableReference tableReference = tableView.getTableReference();
            return tableReference.getKeyspace().equals(keyspace)
                    && tableReference.getTable().equals(table);
        };
    }
}

