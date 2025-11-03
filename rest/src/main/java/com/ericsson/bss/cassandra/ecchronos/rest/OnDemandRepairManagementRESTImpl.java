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
import java.util.Collection;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Collections;
import java.util.Set;

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
@SuppressWarnings("PMD.GodClass")
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
            final RepairType repairType,
            @RequestParam(required = false, defaultValue = "false")
            @Parameter(description = "Confirm that repair is required for all nodes.")
            final boolean all,
            @RequestParam(required = false, defaultValue = "false")
            @Parameter(description = "Force repair of TWCS tables, which are normally ignored.")
            final boolean forceRepairTWCS)

    {
        return ResponseEntity.ok(runOnDemandRepair(nodeID, keyspace, table, getRepairTypeOrDefault(repairType), all,
                forceRepairTWCS));
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

        Collection<Predicate<OnDemandRepairJobView>> filters = new ArrayList<Predicate<OnDemandRepairJobView>>();

        if (keyspace != null)
        {
            Predicate<OnDemandRepairJobView> keyspaceFilter = job -> keyspace.equals(job.getTableReference().getKeyspace());
            filters.add(keyspaceFilter);
            if (table != null)
            {
                Predicate<OnDemandRepairJobView> tableFilter = job -> table.equals(job.getTableReference().getTable());
                filters.add(tableFilter);
            }
        }
        if (hostId != null)
        {
            UUID host = parseIdOrThrow(hostId);
            Predicate<OnDemandRepairJobView> hostFilter = job -> host.equals(job.getNodeId());
            filters.add(hostFilter);
        }
        Predicate<OnDemandRepairJobView> filter = filters.stream().reduce(Predicate::and).orElse(x -> true);
        return getClusterWideOnDemandJobs(filter);
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
            final RepairType repairType,
            final boolean all,
            final boolean forceRepairTWCS)
    {
        try
        {
            List<OnDemandRepair> onDemandRepairs;
            checkValidClusterRun(nodeID, all, keyspace, table);

            UUID nodeUUID = nodeID == null  ? null : parseIdOrThrow(nodeID);
            if (nodeUUID != null  && myDistributedNativeConnectionProvider.getNodes().get(nodeUUID) == null)
            {
                throw new ResponseStatusException(BAD_REQUEST, "Node specified is not a valid node");
            }
            if (keyspace != null)
            {
                onDemandRepairs = getOnDemandRepairsForKeyspace(keyspace, table, repairType, nodeUUID, forceRepairTWCS);
            }
            else
            {
                onDemandRepairs = runLocalOrCluster(nodeUUID, repairType, myTableReferenceFactory.forCluster(), forceRepairTWCS);
            }
            return onDemandRepairs;
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private void checkValidClusterRun(final String nodeID, final boolean all, final String keyspace, final String table)
    {
        if (nodeID == null && !all)
        {
            throw new ResponseStatusException(BAD_REQUEST, "If a node is not specified then parameter all should be true");
        }
        if (keyspace == null && table != null)
        {
            throw new ResponseStatusException(BAD_REQUEST, "Keyspace must be provided if table is provided");
        }

    }
    private List<OnDemandRepair> getOnDemandRepairsForKeyspace(final String keyspace,
                                                               final String table,
                                                               final RepairType repairType,
                                                               final UUID nodeUUID,
                                                               final boolean forceRepairTWCS) throws EcChronosException
    {
        List<OnDemandRepair> onDemandRepairs;
        if (table != null)
        {
            TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
            if (tableReference == null)
            {
                throw new ResponseStatusException(NOT_FOUND,
                        "Table " + keyspace + "." + table + " does not exist");
            }
            if (rejectForTWCS(tableReference, forceRepairTWCS))
            {
                throw new ResponseStatusException(BAD_REQUEST,
                        "Table " + keyspace + "." + table + " uses TWCS");
            }
            onDemandRepairs = runLocalOrCluster(nodeUUID, repairType,
                    Collections.singleton(myTableReferenceFactory.forTable(keyspace, table)), forceRepairTWCS);
        }
        else
        {
            onDemandRepairs = runLocalOrCluster(nodeUUID, repairType,
                    myTableReferenceFactory.forKeyspace(keyspace), forceRepairTWCS);
        }
        return onDemandRepairs;
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
    private Boolean rejectForTWCS(final TableReference tableReference, final boolean forceRepairTWCS)
    {
        return (!forceRepairTWCS &&  tableReference.getTwcs()
                && myOnDemandRepairScheduler.getRepairConfiguration().getIgnoreTWCSTables());

    }

    private List<OnDemandRepair> runLocalOrCluster(
            final UUID nodeID,
            final RepairType repairType,
            final Set<TableReference> tables,
            final boolean forceRepairTWCS)
            throws EcChronosException
    {
        Collection<Node> availableNodes;
        if (nodeID == null)
        {
            availableNodes = myDistributedNativeConnectionProvider.getNodes().values();
        }
        else
        {
            availableNodes = new ArrayList<Node>();
            availableNodes.add(myDistributedNativeConnectionProvider.getNodes().get(nodeID));
        }

        List<OnDemandRepair> onDemandRepairs = new ArrayList<>();
        for (TableReference tableReference : tables)
        {
            for (Node eachNode : availableNodes)
            {
                if (!rejectForTWCS(tableReference, forceRepairTWCS) && myReplicatedTableProvider.accept(eachNode, tableReference.getKeyspace()))
                {
                    onDemandRepairs.add(new OnDemandRepair(
                            myOnDemandRepairScheduler.scheduleJob(tableReference, repairType, eachNode.getHostId())));
                }
            }
        }
        return onDemandRepairs;
    }
}
