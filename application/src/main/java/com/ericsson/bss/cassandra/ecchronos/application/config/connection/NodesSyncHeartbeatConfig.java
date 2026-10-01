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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for the {@code NodeSyncHeartbeatService}, responsible for periodically renewing
 * this instance's rows in the {@code nodes_sync} table with a TTL so that they self-heal (expire)
 * once the instance stops running (issue #1826).
 *
 * <p>Both {@code interval} and {@code ttl} are expressed in the same {@code unit}. The TTL is
 * converted to seconds internally when applied as {@code USING TTL}. The TTL must be larger than
 * the interval so that a single missed heartbeat does not prematurely expire a live node's row;
 * by default the TTL is 3x the interval (1 hour interval, 3 hours TTL).</p>
 */
public final class NodesSyncHeartbeatConfig
{
    private static final long DEFAULT_INTERVAL = 1;
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.HOURS;
    /** Factor applied to the interval to derive the TTL when none is configured. */
    private static final long DEFAULT_TTL_FACTOR = 3;

    private long myInterval = DEFAULT_INTERVAL;
    private Long myTtl = null;
    private TimeUnit myUnit = DEFAULT_UNIT;

    /** Default constructor for Jackson deserialization. */
    public NodesSyncHeartbeatConfig()
    {
    }

    /**
     * Sets the interval between heartbeats, expressed in the configured unit.
     *
     * @param interval the interval in the configured unit, or null to keep the default.
     */
    @JsonProperty("interval")
    public void setInterval(final Long interval)
    {
        if (interval != null)
        {
            myInterval = interval;
        }
    }

    /**
     * Sets the TTL applied to heartbeat writes, expressed in the same unit as the interval. When
     * not set, the TTL defaults to {@value #DEFAULT_TTL_FACTOR}x the interval.
     *
     * @param ttl the TTL in the configured unit, or null to use the derived default.
     */
    @JsonProperty("ttl")
    public void setTtl(final Long ttl)
    {
        myTtl = ttl;
    }

    /**
     * Sets the time unit applied to both the interval and the TTL.
     *
     * @param unit the unit name (e.g. "seconds", "minutes"), or null/blank to keep the default.
     */
    @JsonProperty("unit")
    public void setUnit(final String unit)
    {
        if (unit != null && !unit.isBlank())
        {
            myUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
        }
    }

    /**
     * Returns the heartbeat interval in milliseconds.
     *
     * @return the interval in milliseconds.
     */
    public long getIntervalInMs()
    {
        validate();
        return myUnit.toMillis(myInterval);
    }

    /**
     * Returns the effective TTL (in seconds) for heartbeat writes, converted from the configured
     * unit. When no TTL is configured it defaults to {@value #DEFAULT_TTL_FACTOR}x the interval.
     * Cassandra's {@code USING TTL} is always expressed in seconds.
     *
     * @return the TTL in seconds.
     */
    public int getTtlInSeconds()
    {
        validate();
        return Math.toIntExact(myUnit.toSeconds(effectiveTtl()));
    }

    /**
     * The TTL in the configured unit: the explicit value if set, otherwise the derived default
     * of {@value #DEFAULT_TTL_FACTOR}x the interval.
     *
     * @return the TTL in the configured unit.
     */
    private long effectiveTtl()
    {
        return myTtl != null ? myTtl : myInterval * DEFAULT_TTL_FACTOR;
    }

    /**
     * Validates that the effective TTL is strictly greater than the interval. The TTL must outlast
     * the interval (by default it is {@value #DEFAULT_TTL_FACTOR}x) so that a single missed
     * heartbeat does not prematurely expire a live node's row. The derived default always
     * satisfies this; validation only rejects an explicitly misconfigured TTL.
     *
     * @throws IllegalArgumentException if an explicit TTL is not greater than the interval.
     */
    private void validate()
    {
        long ttl = effectiveTtl();
        if (ttl <= myInterval)
        {
            throw new IllegalArgumentException(String.format(
                    "nodesSyncHeartbeat.ttl (%d) must be greater than nodesSyncHeartbeat.interval (%d) "
                            + "in unit %s; the TTL must outlast the heartbeat interval so a live node's "
                            + "row is not prematurely expired", ttl, myInterval, myUnit));
        }
    }
}
