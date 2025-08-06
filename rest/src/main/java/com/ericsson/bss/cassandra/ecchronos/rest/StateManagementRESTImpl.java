/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.NodeSyncState;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.INTERNAL_MANAGEMENT_ENDPOINT_PREFIX;

import java.util.ArrayList;
import java.util.List;

/**
 * REST controller for internal state of ecChronos instance.
 */
@Tag(name = "State-Management", description = "Management of internal state")
@RestController
public class StateManagementRESTImpl implements StateManagementREST
{
    @Autowired
    private final EccNodesSync myEccNodesSync;

    public StateManagementRESTImpl(final EccNodesSync eccNodesSync)
    {
        myEccNodesSync = eccNodesSync;
    }

    @Override
    @GetMapping(value = INTERNAL_MANAGEMENT_ENDPOINT_PREFIX + "/nodes", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-nodes", description = "Get nodes managed by local instance.",
            summary = "Get nodes managed by local instance.")
    public final ResponseEntity<List<NodeSyncState>> getNodes(
            @RequestParam(required = false)
            @Parameter(description = "Only return nodes managed by the local instance for the specified datacenter.")
            final String datacenter
    )
    {
        return ResponseEntity.ok(getNodesAndFormat(datacenter));
    }

    private List<NodeSyncState> getNodesAndFormat(final String datacenter)
    {
        ResultSet rs = datacenter == null
                ? myEccNodesSync.getAllByLocalInstance() : myEccNodesSync.getAllByLocalAndDCInstance(datacenter);

        List<NodeSyncState> nodesList = new ArrayList<>();

        rs.forEach(row ->
        {
            NodeSyncState nodeSyncState = new NodeSyncState(row);
            nodesList.add(nodeSyncState);
        });
        return nodesList;
    }
}
