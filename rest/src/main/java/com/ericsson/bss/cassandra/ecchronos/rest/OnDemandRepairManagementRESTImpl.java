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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;


import java.util.List;
import java.util.UUID;
import java.util.Collections;
import java.util.Collection;
import java.util.Set;
import java.util.ArrayList;

import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.parseIdOrThrow;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * REST Controller for managing on-demand repair operations in ecChronos,
 * allowing creation, retrieval, and filtering of repair jobs.
 */
@Tag(name = "Repair-Management", description = "Management of repairs")
@RestController
public class OnDemandRepairManagementRESTImpl implements OnDemandRepairManagementREST
{
    private final OnDemandRepairScheduler myOnDemandRepairScheduler;

    private final TableReferenceFactory myTableReferenceFactory;

    private final ReplicatedTableProvider myReplicatedTableProvider;

    @Autowired
    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;

    public OnDemandRepairManagementRESTImpl(
            final OnDemandRepairScheduler demandRepairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final ReplicatedTableProvider replicatedTableProvider,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider)
    {
        myOnDemandRepairScheduler = demandRepairScheduler;
        myTableReferenceFactory = tableReferenceFactory;
        myReplicatedTableProvider = replicatedTableProvider;
        myDistributedNativeConnectionProvider = distributedNativeConnectionProvider;
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/repairs", produces = MediaType.APPLICATION_JSON_VALUE)
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
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/repairs/{nodeID}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repairs-by-id",
            description = "Get manual repairs matching the id which are running/completed/failed.",
            summary = "Get manual repairs matching the id.")
    public final ResponseEntity<List<OnDemandRepair>> getRepairs(
            @PathVariable
            @Parameter(description = "Only return repairs matching the specified nodeID.")
            final String nodeID,
            @RequestParam(required = false)
            @Parameter(description = "Only return repairs matching the specified JobID.")
            final String jobID)
    {
        return ResponseEntity.ok(getListOfOnDemandRepairs(nodeID, jobID));
    }

    @Override
    @PostMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/repairs", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "run-repair",
            description = "Run a manual repair",
            summary = "Run a manual repair.")
    public final ResponseEntity<List<OnDemandRepair>> runRepair(
            @RequestParam(required = false)
            @Parameter(description = "The node to run repair.")
            final String nodeID,
            @RequestParam(required = false)
            @Parameter(description = "The keyspace to run repair for, mandatory if 'table' is provided.")
            final String keyspace,
            @RequestParam(required = false)
            @Parameter(description = "The table to run repair for.")
            final String table,
            @RequestParam(required = false)
            @Parameter(description = "The type of the repair, defaults to vnode.")
            final RepairType repairType)
    {
        return ResponseEntity.ok(runOnDemandRepair(nodeID, keyspace, table, getRepairTypeOrDefault(repairType)));
    }

    private RepairType getRepairTypeOrDefault(final RepairType repairType)
    {
        if (repairType == null)
        {
            return RepairType.VNODE;
        }
        return repairType;
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
                        && host.equals(job.getNodeId()));
            }
            if (hostId == null)
            {
                return getClusterWideOnDemandJobs(
                        job -> keyspace.equals(job.getTableReference().getKeyspace()));
            }
            UUID host = parseIdOrThrow(hostId);
            return getClusterWideOnDemandJobs(
                    job -> keyspace.equals(job.getTableReference().getKeyspace())
                            && host.equals(job.getNodeId()));
        }
        else if (table == null)
        {
            if (hostId == null)
            {
                return getClusterWideOnDemandJobs(job -> true);
            }
            UUID host = parseIdOrThrow(hostId);
            return getClusterWideOnDemandJobs(job -> host.equals(job.getNodeId()));
        }
        throw new ResponseStatusException(BAD_REQUEST);
    }

    private List<OnDemandRepair> getListOfOnDemandRepairs(final String nodeID, final String jobID)
    {

        UUID myNodeID = parseIdOrThrow(nodeID);
        List<OnDemandRepair> repairJobs;
        if (jobID != null)
        {
            UUID myJobID = parseIdOrThrow(jobID);
            repairJobs = getOnDemandJobs(myNodeID).stream().filter(job -> myJobID.equals(job.jobID)).toList();
            if (repairJobs.isEmpty())
            {
                throw new ResponseStatusException(NOT_FOUND);
            }
            return repairJobs;
        }
        repairJobs = getOnDemandJobs(myNodeID);
        return repairJobs;
    }

    private List<OnDemandRepair> runOnDemandRepair(
            final String nodeID,
            final String keyspace, final String table,
            final RepairType repairType)
    {
        try
        {
            List<OnDemandRepair> onDemandRepairs;

            UUID nodeUUID = nodeID == null  ? null : parseIdOrThrow(nodeID);

            if (keyspace != null)
            {
                onDemandRepairs = getOnDemandRepairsForKeyspace(keyspace, table, repairType, nodeUUID);
            }
            else
            {
                if (table != null)
                {
                    throw new ResponseStatusException(BAD_REQUEST, "Keyspace must be provided if table is provided");
                }
                onDemandRepairs = runLocalOrCluster(nodeUUID, repairType, myTableReferenceFactory.forCluster());
            }
            return onDemandRepairs;
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private List<OnDemandRepair> getOnDemandRepairsForKeyspace(String keyspace, String table, RepairType repairType, UUID nodeUUID) throws EcChronosException {
        List<OnDemandRepair> onDemandRepairs;
        if (table != null)
        {
            TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
            if (tableReference == null)
            {
                throw new ResponseStatusException(NOT_FOUND,
                        "Table " + keyspace + "." + table + " does not exist");
            }
            onDemandRepairs = runLocalOrCluster(nodeUUID, repairType,
                    Collections.singleton(myTableReferenceFactory.forTable(keyspace, table)));
        }
        else
        {
            onDemandRepairs = runLocalOrCluster(nodeUUID, repairType,
                    myTableReferenceFactory.forKeyspace(keyspace));
        }
        return onDemandRepairs;
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

    private List<OnDemandRepair> getOnDemandJobs(
            final UUID nodeID)
    {
        return myOnDemandRepairScheduler.getAllRepairJobs(nodeID).stream()
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());
    }

    private List<OnDemandRepair> runLocalOrCluster(
            final UUID nodeID,
            final RepairType repairType,
            final Set<TableReference> tables)
            throws EcChronosException
    {
        if (nodeID == null)
        {
            return runForCluster(repairType, tables);
        }
        List<OnDemandRepair> onDemandRepairs = new ArrayList<>();
        Node node = myDistributedNativeConnectionProvider.getNodes().get(nodeID);
        for (TableReference tableReference : tables)
        {
           if (myReplicatedTableProvider.accept(node, tableReference.getKeyspace()))
            {
                onDemandRepairs.add(new OnDemandRepair(
                        myOnDemandRepairScheduler.scheduleJob(tableReference, repairType, nodeID)));
            }
        }
        return onDemandRepairs;
    }
    private List<OnDemandRepair> runForCluster(
            final RepairType repairType,
            final Set<TableReference> tables)
            throws EcChronosException
    {
        List<OnDemandRepair> onDemandRepairs = new ArrayList<>();
        for (TableReference tableReference : tables)
        {
            Collection<Node> availableNodes = myDistributedNativeConnectionProvider.getNodes().values();
            for (Node eachNode : availableNodes)
            {
                if (myReplicatedTableProvider.accept(eachNode, tableReference.getKeyspace()))
                {
                    onDemandRepairs.add(new OnDemandRepair(
                            myOnDemandRepairScheduler.scheduleJob(tableReference, repairType, eachNode.getHostId())));
                }
            }
        }
        return onDemandRepairs;
    }
}
