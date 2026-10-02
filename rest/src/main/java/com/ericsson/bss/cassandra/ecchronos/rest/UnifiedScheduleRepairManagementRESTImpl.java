/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.UnifiedSchedule;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.parseIdOrThrow;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * REST controller for the consolidated {@code UNIFIED_VNODE} repair schedules, which coordinate one table
 * across all of its nodes under a single job id.
 * <p>
 * Three levels of detail are exposed:
 * <ol>
 *     <li>{@code /unified-schedules} — one {@link UnifiedSchedule} aggregate per table/job (worst-case status
 *     across nodes, average ratio), navigable by keyspace+table;</li>
 *     <li>{@code /unified-schedules/{jobID}/nodes} — the per-node {@link Schedule}s of one job;</li>
 *     <li>{@code /unified-schedules/{jobID}/nodes/{nodeID}} — a single node's {@link Schedule}, optionally
 *     {@code full} to include vnode states.</li>
 * </ol>
 */
@Tag(name = "Repair-Management", description = "Management of repairs")
@RestController
public class UnifiedScheduleRepairManagementRESTImpl
{
    @Autowired
    private final RepairScheduler myRepairScheduler;

    /**
     * Constructs the unified schedule repair management REST controller.
     *
     * @param repairScheduler the repair scheduler (the routing scheduler exposing all repair types).
     */
    public UnifiedScheduleRepairManagementRESTImpl(final RepairScheduler repairScheduler)
    {
        myRepairScheduler = repairScheduler;
    }

    /**
     * Get the table-level aggregates of all unified repair jobs, optionally filtered by keyspace/table.
     *
     * @param keyspace optional keyspace filter (mandatory if {@code table} is provided).
     * @param table optional table filter.
     * @return the list of unified schedule aggregates.
     */
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/unified-schedules",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-unified-schedules", description = "Get unified (multi-node) schedules",
            summary = "Get unified schedules")
    public final ResponseEntity<List<UnifiedSchedule>> getUnifiedSchedules(
            @RequestParam(required = false)
            @Parameter(description = "Filter on this keyspace, mandatory if 'table' is provided.")
            final String keyspace,
            @RequestParam(required = false)
            @Parameter(description = "Filter on this table.")
            final String table)
    {
        if (keyspace == null && table != null)
        {
            throw new ResponseStatusException(BAD_REQUEST);
        }
        List<UnifiedSchedule> aggregates = new ArrayList<>();
        for (List<ScheduledRepairJobView> jobViews : groupUnifiedJobs(keyspace, table).values())
        {
            aggregates.add(new UnifiedSchedule(jobViews));
        }
        return ResponseEntity.ok(aggregates);
    }

    /**
     * Get the per-node schedules of a single unified job.
     *
     * @param jobID the unified job id.
     * @return the per-node schedules (one per node of the job).
     */
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/unified-schedules/{jobID}/nodes",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-unified-schedule-nodes", description = "Get the nodes of a unified schedule",
            summary = "Get unified schedule nodes")
    public final ResponseEntity<List<Schedule>> getUnifiedScheduleNodes(
            @PathVariable
            @Parameter(description = "The id of the unified job.")
            final String jobID)
    {
        List<ScheduledRepairJobView> jobViews = viewsForJob(parseIdOrThrow(jobID));
        List<Schedule> schedules = jobViews.stream().map(Schedule::new).collect(Collectors.toList());
        return ResponseEntity.ok(schedules);
    }

    /**
     * Get a single node's schedule within a unified job, optionally with full vnode detail.
     *
     * @param jobID the unified job id.
     * @param nodeID the node id to filter to.
     * @param full whether to include per-vnode detail.
     * @return the node's schedule within the unified job.
     */
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/unified-schedules/{jobID}/nodes/{nodeID}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-unified-schedule-node", description = "Get one node of a unified schedule",
            summary = "Get unified schedule node")
    public final ResponseEntity<Schedule> getUnifiedScheduleNode(
            @PathVariable
            @Parameter(description = "The id of the unified job.")
            final String jobID,
            @PathVariable
            @Parameter(description = "The id of the node.")
            final String nodeID,
            @RequestParam(required = false)
            @Parameter(description = "Decides if a 'full schedule' (with vnode states) should be returned.")
            final boolean full)
    {
        UUID nodeUUID = parseIdOrThrow(nodeID);
        ScheduledRepairJobView view = viewsForJob(parseIdOrThrow(jobID)).stream()
                .filter(jobView -> nodeUUID.equals(jobView.getNodeId()))
                .findFirst()
                .orElseThrow(() -> new ResponseStatusException(NOT_FOUND));
        return ResponseEntity.ok(new Schedule(view, full));
    }

    private List<ScheduledRepairJobView> viewsForJob(final UUID jobId)
    {
        List<ScheduledRepairJobView> jobViews = unifiedViews().stream()
                .filter(view -> jobId.equals(view.getJobId()))
                .collect(Collectors.toList());
        if (jobViews.isEmpty())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return jobViews;
    }

    private Map<UUID, List<ScheduledRepairJobView>> groupUnifiedJobs(final String keyspace, final String table)
    {
        Map<UUID, List<ScheduledRepairJobView>> grouped = new LinkedHashMap<>();
        for (ScheduledRepairJobView view : unifiedViews())
        {
            if (!matchesFilter(view, keyspace, table))
            {
                continue;
            }
            grouped.computeIfAbsent(view.getJobId(), k -> new ArrayList<>()).add(view);
        }
        return grouped;
    }

    private List<ScheduledRepairJobView> unifiedViews()
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(view -> RepairType.UNIFIED_VNODE.equals(view.getRepairType()))
                .collect(Collectors.toList());
    }

    private static boolean matchesFilter(final ScheduledRepairJobView view, final String keyspace, final String table)
    {
        if (keyspace == null)
        {
            return true;
        }
        if (!keyspace.equals(view.getTableReference().getKeyspace()))
        {
            return false;
        }
        return table == null || table.equals(view.getTableReference().getTable());
    }
}
