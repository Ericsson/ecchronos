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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * When updating the path it should also be updated in the OSGi component.
 */
@Tag(name = "repair-management")
@RestController
@OpenAPIDefinition(info = @Info(
        title = "ecChronos REST API",
        description = "ecChronos REST API can be used to view the status of schedules and repairs as well as run manual repairs",
        license = @License(
                name = "Apache 2.0",
                url = "https://www.apache.org/licenses/LICENSE-2.0"),
        version = "1.0.0"))
public class RepairManagementRESTImpl implements RepairManagementREST
{
    private static final String ROOT = "/repair-management/";
    private static final String PROTOCOL_VERSION = "v2";
    private static final String ENDPOINT_PREFIX = ROOT + PROTOCOL_VERSION;

    @Autowired
    private final RepairScheduler myRepairScheduler;

    @Autowired
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Autowired
    private final TableReferenceFactory myTableReferenceFactory;

    @Autowired
    private final ReplicatedTableProvider myReplicatedTableProvider;

    public RepairManagementRESTImpl(RepairScheduler repairScheduler, OnDemandRepairScheduler demandRepairScheduler,
            TableReferenceFactory tableReferenceFactory, ReplicatedTableProvider replicatedTableProvider)
    {
        myRepairScheduler = repairScheduler;
        myOnDemandRepairScheduler = demandRepairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
        myReplicatedTableProvider = replicatedTableProvider;
    }

    @Override
    @GetMapping(value = ENDPOINT_PREFIX + "/schedules",
                produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-schedules")
    public ResponseEntity<List<Schedule>> getSchedules(@RequestParam(required = false) String keyspace,
                                                       @RequestParam(required = false) String table)
    {
        if (keyspace != null)
        {
            if (table != null)
            {
                List<Schedule> repairJobs = getScheduledRepairJobs(forTable(keyspace, table));
                return ResponseEntity.ok(repairJobs);
            }
            List<Schedule> repairJobs = getScheduledRepairJobs(
                    job -> keyspace.equals(job.getTableReference().getKeyspace()));
            return ResponseEntity.ok(repairJobs);
        }
        else if (table == null)
        {
            List<Schedule> repairJobs = getScheduledRepairJobs(job -> true);
            return ResponseEntity.ok(repairJobs);
        }
        throw new ResponseStatusException(BAD_REQUEST);
    }

    @Override
    @GetMapping(value = ENDPOINT_PREFIX + "/schedules/{id}",
                produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-schedules-by-id")
    public ResponseEntity<Schedule> getSchedules(@PathVariable String id, @RequestParam(required = false) boolean full)
    {

        UUID uuid;
        try
        {
            uuid = UUID.fromString(id);
        }
        catch (IllegalArgumentException e)
        {
            throw new ResponseStatusException(BAD_REQUEST, BAD_REQUEST.getReasonPhrase(), e);
        }
        Optional<RepairJobView> repairJobView = getScheduleView(uuid);
        if (!repairJobView.isPresent())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return ResponseEntity.ok(new Schedule(repairJobView.get(), full));

    }


    @Override
    @GetMapping(value = ENDPOINT_PREFIX + "/repairs",
                produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repairs")
    public ResponseEntity<List<OnDemandRepair>> getRepairs(@RequestParam(required = false) String keyspace,
                                                           @RequestParam(required = false) String table,
                                                           @RequestParam(required = false) String hostId)
    {
        if (keyspace != null)
        {
            if (table != null)
            {
                if (hostId == null)
                {
                    List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(forTable(keyspace, table));
                    return ResponseEntity.ok(repairJobs);
                }
                UUID host = parseIdOrThrow(hostId);
                List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> keyspace.equals(job.getTableReference().getKeyspace()) &&
                                                                                    table.equals(job.getTableReference().getTable()) &&
                                                                                    host.equals(job.getHostId()));
                return ResponseEntity.ok(repairJobs);
            }
            if (hostId == null)
            {
                List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(
                        job -> keyspace.equals(job.getTableReference().getKeyspace()));
                return ResponseEntity.ok(repairJobs);
            }
            UUID host = parseIdOrThrow(hostId);
            List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(
                    job -> keyspace.equals(job.getTableReference().getKeyspace()) &&
                           host.equals(job.getHostId()));
            return ResponseEntity.ok(repairJobs);
        }
        else if (table == null)
        {
            if (hostId == null)
            {
                List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> true);
                return ResponseEntity.ok(repairJobs);
            }
            UUID host = parseIdOrThrow(hostId);
            List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> host.equals(job.getHostId()));
            return ResponseEntity.ok(repairJobs);
        }
        throw new ResponseStatusException(BAD_REQUEST);
    }

    @Override
    @GetMapping(value = ENDPOINT_PREFIX + "/repairs/{id}",
                produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repairs-by-id")
    public ResponseEntity<List<OnDemandRepair>> getRepairs(@PathVariable String id,
                                                           @RequestParam(required = false) String hostId)
    {
        UUID uuid = parseIdOrThrow(id);
        if (hostId == null)
        {
            List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> uuid.equals(job.getId()));
            if (repairJobs.isEmpty())
            {
                throw new ResponseStatusException(NOT_FOUND);
            }
            return ResponseEntity.ok(repairJobs);
        }
        UUID host = parseIdOrThrow(hostId);
        List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> uuid.equals(job.getId()) && host.equals(job.getHostId()));
        if (repairJobs.isEmpty())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return ResponseEntity.ok(repairJobs);
    }

    private UUID parseIdOrThrow(String id)
    {
        try
        {
            UUID uuid = UUID.fromString(id);
            return uuid;
        }
        catch (IllegalArgumentException e)
        {
            throw new ResponseStatusException(BAD_REQUEST, BAD_REQUEST.getReasonPhrase(), e);
        }
    }


    @Override
    @PostMapping(value = ENDPOINT_PREFIX + "/repairs",
                 produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "trigger-repair")
    public ResponseEntity<List<OnDemandRepair>> triggerRepair(@RequestParam(required = false) String keyspace,
                                                              @RequestParam(required = false) String table,
                                                              @RequestParam(required = false) boolean isLocal)
    {
        try
        {
            List<OnDemandRepair> onDemandRepairs;
            if (keyspace != null)
            {
                if (table != null)
                {
                    TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
                    if (tableReference == null)
                    {
                        throw new ResponseStatusException(NOT_FOUND, "Table " + keyspace + "." + table + " does not exist");
                    }
                    onDemandRepairs = runLocalOrCluster(isLocal, Collections.singleton(myTableReferenceFactory.forTable(keyspace, table)));
                }
                else
                {
                    onDemandRepairs = runLocalOrCluster(isLocal, myTableReferenceFactory.forKeyspace(keyspace));
                }
            }
            else
            {
                if (table != null)
                {
                    throw new ResponseStatusException(BAD_REQUEST, "Keyspace must be provided if table is provided");
                }
                onDemandRepairs = runLocalOrCluster(isLocal, myTableReferenceFactory.forCluster());
            }
            return ResponseEntity.ok(onDemandRepairs);
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private List<OnDemandRepair> runLocalOrCluster(boolean isLocal, Set<TableReference> tables)
            throws EcChronosException
    {
        List<OnDemandRepair> onDemandRepairs = new ArrayList<>();
        for (TableReference tableReference : tables)
        {
            if (isLocal)
            {
                if (myReplicatedTableProvider.accept(tableReference.getKeyspace()))
                {
                    onDemandRepairs.add(new OnDemandRepair(myOnDemandRepairScheduler.scheduleJob(tableReference)));
                }
            }
            else
            {
                if (myReplicatedTableProvider.accept(tableReference.getKeyspace()))
                {
                    List<RepairJobView> repairJobView = myOnDemandRepairScheduler.scheduleClusterWideJob(
                            tableReference);
                    onDemandRepairs.addAll(
                            repairJobView.stream().map(OnDemandRepair::new).collect(Collectors.toList()));
                }
            }
        }
        return onDemandRepairs;
    }

    private List<Schedule> getScheduledRepairJobs(Predicate<RepairJobView> filter)
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(filter)
                .map(Schedule::new)
                .collect(Collectors.toList());
    }

    private List<OnDemandRepair> getClusterWideOnDemandJobs(Predicate<RepairJobView> filter)
    {
        return myOnDemandRepairScheduler.getAllClusterWideRepairJobs().stream()
                .filter(filter)
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());
    }

    private Optional<RepairJobView> getScheduleView(UUID id)
    {
        return myRepairScheduler.getCurrentRepairJobs().stream()
                .filter(job -> job.getId().equals(id)).findFirst();
    }

    private Predicate<RepairJobView> forTable(String keyspace, String table)
    {
        return tableView ->
        {
            TableReference tableReference = tableView.getTableReference();
            return tableReference.getKeyspace().equals(keyspace)
                    && tableReference.getTable().equals(table);
        };
    }
}
