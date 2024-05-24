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

import java.io.File;

public class StatisticsConfig
{
    private boolean myIsEnabled = true;
    private File myOutputDirectory = new File("./statistics");
    private ReportingConfigs myReportingConfigs = new ReportingConfigs();
    private String myMetricsPrefix = "";
    private int myRepairFailuresCount = 0;
    private int myRepairFailuresTimeWindowInMinutes = 0;

    @JsonProperty("enabled")
    public final boolean isEnabled()
    {
        boolean isAnyReportingEnabled = myReportingConfigs.isFileReportingEnabled()
                || myReportingConfigs.isJmxReportingEnabled()
                || myReportingConfigs.isHttpReportingEnabled();
        return myIsEnabled && isAnyReportingEnabled;
    }

    @JsonProperty("directory")
    public final File getOutputDirectory()
    {
        return myOutputDirectory;
    }

    @JsonProperty("reporting")
    public final ReportingConfigs getReportingConfigs()
    {
        return myReportingConfigs;
    }

    @JsonProperty("prefix")
    public final String getMetricsPrefix()
    {
        return myMetricsPrefix;
    }

    @JsonProperty("repair_failures_count")
    public final int getRepairFailuresCount()
    {
        return myRepairFailuresCount;
    }

    @JsonProperty("repair_failure_time_window")
    public final int getRepairFailuresTimeWindowInMinutes()
    {
        return myRepairFailuresTimeWindowInMinutes;
    }

    @JsonProperty("enabled")
    public final void setEnabled(final boolean enabled)
    {
        myIsEnabled = enabled;
    }

    @JsonProperty("directory")
    public final void setOutputDirectory(final String outputDirectory)
    {
        myOutputDirectory = new File(outputDirectory);
    }

    @JsonProperty("reporting")
    public final void setReportingConfigs(final ReportingConfigs reportingConfigs)
    {
        myReportingConfigs = reportingConfigs;
    }

    @JsonProperty("prefix")
    public final void setMetricsPrefix(final String metricsPrefix)
    {
        myMetricsPrefix = metricsPrefix;
    }

    @JsonProperty("repair_failures_count")
    public final void setRepairFailuresCount(final int repairFailureCount)
    {
        myRepairFailuresCount = repairFailureCount;
    }

    @JsonProperty("repair_failures_time_window_in_minutes")
    public final void setRepairFailuresTimeWindowInMinutes(final int repairFailureTimeWindow)
    {
        myRepairFailuresTimeWindowInMinutes = repairFailureTimeWindow;
    }
}



