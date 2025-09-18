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

import com.ericsson.bss.cassandra.ecchronos.core.state.ApplicationStateHolder;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * REST controller for application state management.
 */
@Tag(name = "State-Management", description = "Management of application state")
@RestController
public final class StateRepairManagementRESTImpl implements StateRepairManagementREST
{

    public StateRepairManagementRESTImpl()
    {
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/state", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-all-state",
               description = "Get all application state entries",
               summary = "Get all state")
    public ResponseEntity<Map<String, Object>> getAllState()
    {
        return ResponseEntity.ok(ApplicationStateHolder.getInstance().getAll());
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/state/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-state-by-key",
               description = "Get a specific state value by key",
               summary = "Get state by key")
    public ResponseEntity<Object> getState(
            @PathVariable
            @Parameter(description = "The state key to retrieve")
            final String key)
    {
        return ApplicationStateHolder.getInstance().get(key)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(NOT_FOUND).build());
    }

}
