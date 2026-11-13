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
package com.ericsson.bss.cassandra.ecchronos.application.logging;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class TestVersionLogbackHeaderLayout
{
    private final VersionLogbackHeaderLayout layout = new VersionLogbackHeaderLayout();

    @Test
    public void testHeaderContainsVersionAndCommit()
    {
        String header = layout.buildHeader("1.1.3", "76394eea");

        assertThat(header).startsWith("# ecChronos 1.1.3 (commit 76394eea) - log opened ");
    }

    @Test
    public void testNullValuesFallBackToUnknown()
    {
        String header = layout.buildHeader(null, null);

        assertThat(header).startsWith(
                "# ecChronos " + VersionLogbackHeaderLayout.UNKNOWN
                        + " (commit " + VersionLogbackHeaderLayout.UNKNOWN + ") - log opened ");
    }

    @Test
    public void testEmptyValuesFallBackToUnknown()
    {
        String header = layout.buildHeader("", "");

        assertThat(header).contains("# ecChronos " + VersionLogbackHeaderLayout.UNKNOWN
                + " (commit " + VersionLogbackHeaderLayout.UNKNOWN + ")");
    }

    @Test
    public void testUnfilteredPlaceholdersFallBackToUnknown()
    {
        String header = layout.buildHeader("${project.version}", "${git.commit.id.abbrev}");

        assertThat(header).contains("# ecChronos " + VersionLogbackHeaderLayout.UNKNOWN
                + " (commit " + VersionLogbackHeaderLayout.UNKNOWN + ")");
    }

    @Test
    public void testGetFileHeaderIsWellFormedAndStable()
    {
        // getFileHeader() reads version.properties from the classpath. Regardless of whether the
        // resource is present/filtered, the header must be well-formed and non-null. Logback calls
        // this on every file open (including rollover), so it must never fail.
        String first = layout.getFileHeader();
        String second = layout.getFileHeader();

        assertThat(first).isNotNull();
        assertThat(first).startsWith("# ecChronos ");
        assertThat(first).contains(" (commit ");
        assertThat(first).contains(") - log opened ");
        assertThat(second).startsWith("# ecChronos ");
    }
}
