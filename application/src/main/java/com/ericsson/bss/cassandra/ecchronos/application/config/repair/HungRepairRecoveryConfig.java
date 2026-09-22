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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for automatic recovery of hung incremental repair sessions (issue #1825).
 * <p>
 * Disabled by default. When enabled, ecChronos periodically scans each managed node's own repair sessions and
 * cancels (coordinator-only) any session that has been stalled longer than the configured threshold.
 */
public class HungRepairRecoveryConfig
{
    private static final int DEFAULT_STALL_THRESHOLD_MINUTES = 30;
    private static final int DEFAULT_SCAN_INTERVAL_MINUTES = 5;

    private boolean myEnabled = false;
    private Interval myStallThreshold = new Interval(DEFAULT_STALL_THRESHOLD_MINUTES, TimeUnit.MINUTES);
    private Interval myScanInterval = new Interval(DEFAULT_SCAN_INTERVAL_MINUTES, TimeUnit.MINUTES);
    private boolean myBypassCoordinatorCheck = false;
    private boolean myForce = false;

    /** Default constructor. */
    public HungRepairRecoveryConfig()
    {
        // Default constructor for Jackson
    }

    /**
     * Whether hung repair session recovery is enabled.
     *
     * @return {@code true} if enabled.
     */
    @JsonProperty("enabled")
    public boolean isEnabled()
    {
        return myEnabled;
    }

    /**
     * Sets whether hung repair session recovery is enabled.
     *
     * @param enabled {@code true} to enable.
     */
    @JsonProperty("enabled")
    public void setEnabled(final boolean enabled)
    {
        myEnabled = enabled;
    }

    /**
     * Gets the stall threshold interval.
     *
     * @return the stall threshold interval.
     */
    @JsonProperty("stall_threshold")
    public Interval getStallThreshold()
    {
        return myStallThreshold;
    }

    /**
     * Sets the stall threshold interval.
     *
     * @param stallThreshold the stall threshold interval.
     */
    @JsonProperty("stall_threshold")
    public void setStallThreshold(final Interval stallThreshold)
    {
        myStallThreshold = stallThreshold;
    }

    /**
     * Gets the scan interval.
     *
     * @return the scan interval.
     */
    @JsonProperty("scan_interval")
    public Interval getScanInterval()
    {
        return myScanInterval;
    }

    /**
     * Sets the scan interval.
     *
     * @param scanInterval the scan interval.
     */
    @JsonProperty("scan_interval")
    public void setScanInterval(final Interval scanInterval)
    {
        myScanInterval = scanInterval;
    }

    /**
     * Gets the stall threshold in milliseconds.
     *
     * @return the stall threshold in milliseconds.
     */
    public long getStallThresholdInMs()
    {
        return myStallThreshold.getInterval(TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the scan interval in milliseconds.
     *
     * @return the scan interval in milliseconds.
     */
    public long getScanIntervalInMs()
    {
        return myScanInterval.getInterval(TimeUnit.MILLISECONDS);
    }

    /**
     * Whether the coordinator check is bypassed. When true, a hung session is failed on every managed node instead
     * of only its coordinator.
     *
     * @return {@code true} if the coordinator check is bypassed.
     */
    @JsonProperty("bypass_coordinator_check")
    public boolean isBypassCoordinatorCheck()
    {
        return myBypassCoordinatorCheck;
    }

    /**
     * Sets whether the coordinator check is bypassed.
     *
     * @param bypassCoordinatorCheck {@code true} to fail hung sessions on every managed node.
     */
    @JsonProperty("bypass_coordinator_check")
    public void setBypassCoordinatorCheck(final boolean bypassCoordinatorCheck)
    {
        myBypassCoordinatorCheck = bypassCoordinatorCheck;
    }

    /**
     * Whether {@code failSession} is invoked with force=true for all requests, regardless of coordinator status.
     *
     * @return {@code true} if force is enabled.
     */
    @JsonProperty("force")
    public boolean isForce()
    {
        return myForce;
    }

    /**
     * Sets whether {@code failSession} is invoked with force=true for all requests.
     *
     * @param force {@code true} to force-fail sessions.
     */
    @JsonProperty("force")
    public void setForce(final boolean force)
    {
        myForce = force;
    }
}
