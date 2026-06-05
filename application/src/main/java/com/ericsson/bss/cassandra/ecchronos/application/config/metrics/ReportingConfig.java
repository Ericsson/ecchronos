/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Set;

/** Configuration for a single metrics reporting target. */
public class ReportingConfig
{
    private boolean myIsEnabled = true;
    private Set<ExcludedMetric> myExcludedMetrics = new HashSet<>();

    /** Default constructor. */
    public ReportingConfig()
    {
    }

    /**
     * Returns whether enabled.
     * @return true if enabled
     */
    @JsonProperty("enabled")
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    /**
     * Sets the enabled.
     * @param enabled whether enabled
     */
    @JsonProperty("enabled")
    public final void setEnabled(final boolean enabled)
    {
        myIsEnabled = enabled;
    }

    /**
     * Returns the excluded metrics.
     * @return the excluded metrics
     */
    @JsonProperty("excludedMetrics")
    public final Set<ExcludedMetric> getExcludedMetrics()
    {
        return myExcludedMetrics;
    }

    /**
     * Sets the excluded metrics.
     * @param excludedMetrics the excluded metrics
     */
    @JsonProperty("excludedMetrics")
    public final void setExcludedMetrics(final Set<ExcludedMetric> excludedMetrics)
    {
        myExcludedMetrics = excludedMetrics;
    }
}
