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

public class ReportingConfig
{
    private boolean myIsEnabled = true;
    private Set<ExcludedMetric> myExcludedMetrics = new HashSet<>();

    @JsonProperty("enabled")
    public final boolean isEnabled()
    {
        return myIsEnabled;
    }

    @JsonProperty("enabled")
    public final void setEnabled(final boolean enabled)
    {
        myIsEnabled = enabled;
    }

    @JsonProperty("excludedMetrics")
    public final Set<ExcludedMetric> getExcludedMetrics()
    {
        return myExcludedMetrics;
    }

    @JsonProperty("excludedMetrics")
    public final void setExcludedMetrics(final Set<ExcludedMetric> excludedMetrics)
    {
        myExcludedMetrics = excludedMetrics;
    }
}
