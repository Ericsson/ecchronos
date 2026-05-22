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

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
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

import java.util.List;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;

/**
 * REST Controller for managing on-demand repair operations in ecChronos,
 * allowing creation, retrieval, and filtering of repair jobs.
 */
@Tag(name = "Repair-Management", description = "Management of repairs")
@RestController
public class OnDemandRepairManagementRESTImpl implements OnDemandRepairManagementREST
{
    private final OnDemandRepairJobOrchestrator myOrchestrator;
    private final OnDemandRepairRequestValidator myValidator;

    @Autowired
    public OnDemandRepairManagementRESTImpl(
            final OnDemandRepairScheduler demandRepairScheduler,
            final TableReferenceFactory tableReferenceFactory,
            final ReplicatedTableProvider replicatedTableProvider,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider)
    {
        myValidator = new OnDemandRepairRequestValidator(
                demandRepairScheduler, replicatedTableProvider, distributedNativeConnectionProvider);
        myOrchestrator = new OnDemandRepairJobOrchestrator(
                demandRepairScheduler, tableReferenceFactory, distributedNativeConnectionProvider, myValidator);
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
        return ResponseEntity.ok(myOrchestrator.getRepairs(keyspace, table, hostId));
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
        return ResponseEntity.ok(myOrchestrator.getRepairs(nodeID, jobID));
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
            final boolean forceRepairTWCS,
            @RequestParam(required = false, defaultValue = "false")
            @Parameter(description = "Force repair of tables disabled in the schedule .")
            final boolean forceRepairDisabled)
    {
        return ResponseEntity.ok(myOrchestrator.runRepair(nodeID, keyspace, table,
                myValidator.getRepairTypeOrDefault(repairType), all, forceRepairTWCS, forceRepairDisabled));
    }
}
