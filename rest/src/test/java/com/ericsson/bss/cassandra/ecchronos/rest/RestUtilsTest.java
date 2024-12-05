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

import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import org.junit.Test;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.getDefaultDurationOrProvided;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.parseIdOrThrow;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestUtilsTest
{
    @Test
    public void testParseUuid()
    {
        UUID uuid = UUID.randomUUID();
        UUID processedId = parseIdOrThrow(uuid.toString());
        assertThat(uuid).isEqualTo(processedId);
    }

    @Test
    public void testParseUuidFails()
    {
        String incorrectId = "incorrectId";
        catchThrowableOfType(() -> parseIdOrThrow(incorrectId), ResponseStatusException.class);
    }

    @Test
    public void testDurationWithSince()
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getGcGraceSeconds()).thenReturn(12);
        long since = 1245L;
        long durationInMs = 1000L;
        Duration duration = Duration.ofMillis(durationInMs);
        Duration processedDuration = getDefaultDurationOrProvided(tableReference, duration, since);
        assertThat(processedDuration).isEqualTo(duration);
    }

    @Test
    public void testDefaultDurationDefault()
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getGcGraceSeconds()).thenReturn(12);
        Duration processedDuration = getDefaultDurationOrProvided(tableReference, null, null);
        assertThat(processedDuration).isEqualTo(Duration.ofMillis(12000));
    }

    @Test
    public void testDurationWithoutSince()
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getGcGraceSeconds()).thenReturn(12);
        long durationInMs = 1000L;
        Duration duration = Duration.ofMillis(durationInMs);
        Duration processedDuration = getDefaultDurationOrProvided(tableReference, duration, null);
        assertThat(processedDuration).isEqualTo(duration);
    }

    @Test
    public void testDurationWithoutDurationReturnsNull()
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getGcGraceSeconds()).thenReturn(12);
        long since = 1245L;
        Duration processedDuration = getDefaultDurationOrProvided(tableReference, null, since);
        assertThat(processedDuration).isNull();
    }
}
