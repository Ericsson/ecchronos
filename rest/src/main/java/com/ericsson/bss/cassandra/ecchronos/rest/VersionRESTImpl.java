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

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.Collections;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for exposing the ecChronos agent version.
 */
@Tag(name = "Version", description = "Retrieve the ecChronos agent version")
@RestController
@PropertySource("classpath:version.properties")
public class VersionRESTImpl implements VersionREST
{
    static final String UNKNOWN_VERSION = "unknown";

    private final String myVersion;

    /**
     * Constructs the version REST controller.
     *
     * @param version the ecChronos agent version, injected from the build-filtered
     *                {@code ecchronos.version} property; defaults to {@code unknown}.
     */
    public VersionRESTImpl(@Value("${ecchronos.version:unknown}") final String version)
    {
        if (version == null || version.isEmpty())
        {
            myVersion = UNKNOWN_VERSION;
        }
        else
        {
            myVersion = version;
        }
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/version", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "version", description = "Get the ecChronos agent version", summary = "Get version")
    public final ResponseEntity<Map<String, String>> getVersion()
    {
        return ResponseEntity.ok(Collections.singletonMap("version", myVersion));
    }
}
