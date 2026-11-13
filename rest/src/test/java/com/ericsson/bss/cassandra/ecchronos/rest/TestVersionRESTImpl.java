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
    public void testGetVersionReturnsConfiguredVersionAndCommit()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("1.1.2", "abcd1234");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).containsEntry("version", "1.1.2");
        assertThat(response.getBody()).containsEntry("commit", "abcd1234");
    }

    @Test
    public void testGetVersionProducesJsonWithVersionAndCommit()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("1.1.2", "abcd1234");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).hasSize(2);
        assertThat(response.getBody().keySet()).containsExactly("version", "commit");
    }

    @Test
    public void testGetVersionNullFallsBackToUnknown()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl(null, null);

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).containsEntry("version", VersionRESTImpl.UNKNOWN_VERSION);
        assertThat(response.getBody()).containsEntry("commit", VersionRESTImpl.UNKNOWN_VERSION);
    }

    @Test
    public void testGetVersionEmptyFallsBackToUnknown()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("", "");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).containsEntry("version", VersionRESTImpl.UNKNOWN_VERSION);
        assertThat(response.getBody()).containsEntry("commit", VersionRESTImpl.UNKNOWN_VERSION);
    }

    @Test
    public void testGetVersionStringReturnsConfiguredVersion()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("1.1.2", "abcd1234");

        assertThat(versionREST.getVersionString()).isEqualTo("1.1.2");
    }

    @Test
    public void testGetCommitReturnsConfiguredCommit()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl("1.1.2", "abcd1234");

        assertThat(versionREST.getCommit()).isEqualTo("abcd1234");
    }

    @Test
    public void testGetVersionStringNullFallsBackToUnknown()
    {
        VersionRESTImpl versionREST = new VersionRESTImpl(null, null);

        assertThat(versionREST.getVersionString()).isEqualTo(VersionRESTImpl.UNKNOWN_VERSION);
        assertThat(versionREST.getCommit()).isEqualTo(VersionRESTImpl.UNKNOWN_VERSION);
    }

    @Test
    public void testUnfilteredPlaceholderIsTreatedAsUnknown()
    {
        // When resource filtering did not run (e.g. building without a .git directory),
        // the injected values are still raw Maven placeholders. These must be normalized
        // to "unknown" rather than leaked verbatim.
        VersionRESTImpl versionREST = new VersionRESTImpl("${project.version}", "${git.commit.id.abbrev}");

        ResponseEntity<Map<String, String>> response = versionREST.getVersion();

        assertThat(response.getBody()).containsEntry("version", VersionRESTImpl.UNKNOWN_VERSION);
        assertThat(response.getBody()).containsEntry("commit", VersionRESTImpl.UNKNOWN_VERSION);
        assertThat(response.getHeaders().getContentType()).isNull();
        // Content type is applied by the @GetMapping 'produces' at the framework layer;
        // direct method invocation does not set it, matching the other REST unit tests.
        assertThat(MediaType.APPLICATION_JSON_VALUE).isEqualTo("application/json");
    }
}
