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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.ENDPOINT_PREFIX;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.parseIdOrThrow;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * When updating the path it should also be updated in the OSGi component.
 */
@Tag(name = "Repair-Management", description = "View the status of repairs as well as run manual repairs")
@RestController
@OpenAPIDefinition(info = @Info(
        title = "REST API",
        license = @License(
                name = "Apache 2.0",
                url = "https://www.apache.org/licenses/LICENSE-2.0"),
        version = "1.0.0"))
public class OnDemandRepairManagementRESTImpl implements OnDemandRepairManagementREST
{
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;

    private final TableReferenceFactory myTableReferenceFactory;

    private final ReplicatedTableProvider myReplicatedTableProvider;

    public OnDemandRepairManagementRESTImpl(final OnDemandRepairScheduler demandRepairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final ReplicatedTableProvider replicatedTableProvider)
    {
        myOnDemandRepairScheduler = demandRepairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
        myReplicatedTableProvider = replicatedTableProvider;
    }

    @Override
    @GetMapping(value = ENDPOINT_PREFIX + "/repairs", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repairs", description = "Get manual repairs which are running/completed/failed.",
            summary = "Get manual repairs.")
    public final ResponseEntity<List<OnDemandRepair>> getRepairs(
            @RequestParam(required = false)
            @Parameter(description = "Only return repairs matching the keyspace, mandatory if 'table' is provided.")
            final String keyspace,
            @RequestParam(required = false)
            @Parameter(description = "Only return repairs matching the table.")
            final String table,
            @RequestParam(required = false)
            @Parameter(description = "Only return repairs matching the hostId.")
            final String hostId)
    {
        return ResponseEntity.ok(getListOfOnDemandRepairs(keyspace, table, hostId));
    }

    @Override
    @GetMapping(value = ENDPOINT_PREFIX + "/repairs/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repairs-by-id",
            description = "Get manual repairs matching the id which are running/completed/failed.",
            summary = "Get manual repairs matching the id.")
    public final ResponseEntity<List<OnDemandRepair>> getRepairs(
            @PathVariable
            @Parameter(description = "Only return repairs matching the id.")
            final String id,
            @RequestParam(required = false)
            @Parameter(description = "Only return repairs matching the hostId.")
            final String hostId)
    {
        return ResponseEntity.ok(getListOfOnDemandRepairs(id, hostId));
    }

    @Override
    @PostMapping(value = ENDPOINT_PREFIX + "/repairs", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "trigger-repair",
            description = "Run a manual repair, if 'isLocal' is not provided this will trigger a cluster-wide repair.",
            summary = "Run a manual repair.")
    public final ResponseEntity<List<OnDemandRepair>> triggerRepair(
            @RequestParam(required = false)
            @Parameter(description = "The keyspace to run repair for, mandatory if 'table' is provided.")
            final String keyspace,
            @RequestParam(required = false)
            @Parameter(description = "The table to run repair for.")
            final String table,
            @RequestParam(required = false)
            @Parameter(description = "Decides if the repair should be only for the local node, i.e not cluster-wide.")
            final boolean isLocal)
    {
        return ResponseEntity.ok(getListOfOnDemandRepairs(keyspace, table, isLocal));
    }


    private List<OnDemandRepair> getListOfOnDemandRepairs(final String keyspace, final String table,
            final String hostId)
    {
        if (keyspace != null)
        {
            if (table != null)
            {
                if (hostId == null)
                {
                    return getClusterWideOnDemandJobs(forTableOnDemand(keyspace, table));
                }
                UUID host = parseIdOrThrow(hostId);
                return getClusterWideOnDemandJobs(job -> keyspace.equals(job.getTableReference().getKeyspace())
                        && table.equals(job.getTableReference().getTable())
                        && host.equals(job.getHostId()));
            }
            if (hostId == null)
            {
                return getClusterWideOnDemandJobs(
                        job -> keyspace.equals(job.getTableReference().getKeyspace()));
            }
            UUID host = parseIdOrThrow(hostId);
            return getClusterWideOnDemandJobs(
                    job -> keyspace.equals(job.getTableReference().getKeyspace())
                            && host.equals(job.getHostId()));
        }
        else if (table == null)
        {
            if (hostId == null)
            {
                return getClusterWideOnDemandJobs(job -> true);
            }
            UUID host = parseIdOrThrow(hostId);
            return getClusterWideOnDemandJobs(job -> host.equals(job.getHostId()));
        }
        throw new ResponseStatusException(BAD_REQUEST);
    }

    private List<OnDemandRepair> getListOfOnDemandRepairs(final String id, final String hostId)
    {
        UUID uuid = parseIdOrThrow(id);
        if (hostId == null)
        {
            List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(
                    job -> uuid.equals(job.getId()));
            if (repairJobs.isEmpty())
            {
                throw new ResponseStatusException(NOT_FOUND);
            }
            return repairJobs;
        }
        UUID host = parseIdOrThrow(hostId);
        List<OnDemandRepair> repairJobs = getClusterWideOnDemandJobs(job -> uuid.equals(job.getId())
                && host.equals(job.getHostId()));
        if (repairJobs.isEmpty())
        {
            throw new ResponseStatusException(NOT_FOUND);
        }
        return repairJobs;
    }

    private List<OnDemandRepair> getListOfOnDemandRepairs(final String keyspace, final String table,
            final boolean isLocal)
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
                        throw new ResponseStatusException(NOT_FOUND,
                                "Table " + keyspace + "." + table + " does not exist");
                    }
                    onDemandRepairs = runLocalOrCluster(
                            isLocal, Collections.singleton(myTableReferenceFactory.forTable(keyspace, table)));
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
            return onDemandRepairs;
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private static Predicate<OnDemandRepairJobView> forTableOnDemand(final String keyspace, final String table)
    {
        return tableView ->
        {
            TableReference tableReference = tableView.getTableReference();
            return tableReference.getKeyspace().equals(keyspace)
                    && tableReference.getTable().equals(table);
        };
    }

    private List<OnDemandRepair> getClusterWideOnDemandJobs(final Predicate<OnDemandRepairJobView> filter)
    {
        return myOnDemandRepairScheduler.getAllClusterWideRepairJobs().stream()
                .filter(filter)
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());
    }

    private List<OnDemandRepair> runLocalOrCluster(final boolean isLocal, final Set<TableReference> tables)
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
                    List<OnDemandRepairJobView> repairJobView = myOnDemandRepairScheduler.scheduleClusterWideJob(
                            tableReference);
                    onDemandRepairs.addAll(
                            repairJobView.stream().map(OnDemandRepair::new).collect(Collectors.toList()));
                }
            }
        }
        return onDemandRepairs;
    }
}
