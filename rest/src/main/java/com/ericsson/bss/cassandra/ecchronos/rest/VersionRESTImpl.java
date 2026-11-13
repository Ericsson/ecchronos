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

import java.util.LinkedHashMap;
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
@PropertySource("classpath:ecchronos-version.properties")
public class VersionRESTImpl implements VersionREST
{
    static final String UNKNOWN_VERSION = "unknown";

    private final String myVersion;

    private final String myCommit;

    /**
     * Constructs the version REST controller.
     *
     * @param version the ecChronos agent version, injected from the build-filtered
     *                {@code ecchronos.version} property; defaults to {@code unknown}.
     * @param commit  the abbreviated HEAD commit SHA at build time, injected from the
     *                build-filtered {@code ecchronos.git.commit} property; defaults to
     *                {@code unknown}.
     */
    public VersionRESTImpl(
            @Value("${ecchronos.version:unknown}") final String version,
            @Value("${ecchronos.git.commit:unknown}") final String commit)
    {
        myVersion = normalize(version);
        myCommit = normalize(commit);
    }

    /**
     * Normalizes a build-filtered value. Null, empty, and unresolved Maven placeholders
     * (values still starting with {@code ${}, e.g. when building without a {@code .git}
     * directory or without resource filtering) are reported as {@link #UNKNOWN_VERSION}.
     *
     * @param value the raw injected value
     * @return the normalized value, never null
     */
    private static String normalize(final String value)
    {
        if (value == null || value.isEmpty() || value.startsWith("${"))
        {
            return UNKNOWN_VERSION;
        }
        return value;
    }

    /**
     * Returns the resolved ecChronos agent version.
     *
     * @return the version string, or {@link #UNKNOWN_VERSION} if it could not be determined.
     */
    public final String getVersionString()
    {
        return myVersion;
    }

    /**
     * Returns the abbreviated HEAD commit SHA the agent was built from.
     *
     * @return the commit SHA, or {@link #UNKNOWN_VERSION} if it could not be determined.
     */
    public final String getCommit()
    {
        return myCommit;
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/version", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "version", description = "Get the ecChronos agent version", summary = "Get version")
    public final ResponseEntity<Map<String, String>> getVersion()
    {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("version", myVersion);
        body.put("commit", myCommit);
        return ResponseEntity.ok(body);
    }
}
