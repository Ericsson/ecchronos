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
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.UUID;

import static org.springframework.http.HttpStatus.BAD_REQUEST;

/**
 * Helper class that contains methods to parse REST information.
 */
public final class RestUtils
{
    public static final String REPAIR_MANAGEMENT_ENDPOINT_PREFIX = "/repair-management";
    public static final String INTERNAL_MANAGEMENT_ENDPOINT_PREFIX = "/state";

    private RestUtils()
    {
        // Utility Class
    }

    public static UUID parseIdOrThrow(final String id)
    {
        try
        {
            UUID uuid = UUID.fromString(id);
            return uuid;
        }
        catch (IllegalArgumentException e)
        {
            throw new ResponseStatusException(BAD_REQUEST, BAD_REQUEST.getReasonPhrase(), e);
        }
    }

    /**
     * Fetches duration provided.
     * if no duration and since are provided, it will fetch the table default
     *
     * @param tableReference the table to fetch the default from
     * @param duration provided duration
     * @param since provided since
     * @return the duration
     */
    public static Duration getDefaultDurationOrProvided(final TableReference tableReference, final Duration duration,
            final Long since)
    {
        Duration singleTableDuration = duration;
        if (duration == null && since == null)
        {
            singleTableDuration = Duration.ofSeconds(tableReference.getGcGraceSeconds());
        }
        return singleTableDuration;
    }
}
