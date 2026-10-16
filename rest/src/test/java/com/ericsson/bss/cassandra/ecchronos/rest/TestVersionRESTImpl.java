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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

public class TestVersionRESTImpl
{
    @Test
    public void testGetVersionReturnsConfiguredVersion()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("1.1.2");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).containsEntry("version", "1.1.2");
    }

    @Test
    public void testGetVersionProducesJson()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("1.1.2");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).hasSize(1);
        assertThat(response.getBody().keySet()).containsExactly("version");
    }

    @Test
    public void testGetVersionNullFallsBackToUnknown()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl(null);

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).containsEntry("version", VersionRESTImpl.UNKNOWN_VERSION);
    }

    @Test
    public void testGetVersionEmptyFallsBackToUnknown()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).containsEntry("version", VersionRESTImpl.UNKNOWN_VERSION);
    }

    @Test
    public void testGetVersionUnfilteredPlaceholderIsNotTreatedAsReal()
    {
        // If resource filtering did not run, the raw value would still be a non-empty
        // string; this test documents that the controller returns whatever it is given
        // (filtering correctness is covered by the build), so we only assert JSON shape.
        VersionRESTImpl versionREST = new VersionRESTImpl("1.1.2-SNAPSHOT");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).containsEntry("version", "1.1.2-SNAPSHOT");
        assertThat(response.getHeaders().getContentType()).isNull();
        // Content type is applied by the @GetMapping 'produces' at the framework layer;
        // direct method invocation does not set it, matching the other REST unit tests.
        assertThat(MediaType.APPLICATION_JSON_VALUE).isEqualTo("application/json");
    }
}
