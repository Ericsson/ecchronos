/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Interval;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class StatisticsConfig
{
    private static final int DEFAULT_FAILURES_TIME_WINDOW_IN_MINUTES = 30;
    private static final int DEFAULT_TRIGGER_INTERVAL_FOR_METRIC_INSPECTION_IN_SECONDS = 5;
    private static final int DEFAULT_REPAIR_FAILURES_COUNT = 5;
    private boolean myIsEnabled = true;

    private File myOutputDirectory = new File("./statistics");
    private ReportingConfigs myReportingConfigs = new ReportingConfigs();
    private String myMetricsPrefix = "";
    private int myRepairFailuresCount = DEFAULT_REPAIR_FAILURES_COUNT;
    private Interval myRepairFailuresTimeWindow = new Interval(DEFAULT_FAILURES_TIME_WINDOW_IN_MINUTES,
            TimeUnit.MINUTES);
    private Interval myTriggerIntervalForMetricInspection = new
            Interval(DEFAULT_TRIGGER_INTERVAL_FOR_METRIC_INSPECTION_IN_SECONDS, TimeUnit.SECONDS);

    @JsonProperty("enabled")
    public final boolean isEnabled()
    {
       return myIsEnabled;
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

    @JsonProperty("repair_failures_time_window")
    public final Interval getRepairFailuresTimeWindow()
    {
        return myRepairFailuresTimeWindow;
    }

    @JsonProperty("trigger_interval_for_metric_inspection")
    public final Interval getTriggerIntervalForMetricInspection()
    {
        return myTriggerIntervalForMetricInspection;
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
    public final void setRepairFailuresCount(final int repairFailuresCount)
    {
        myRepairFailuresCount = repairFailuresCount;
    }

    @JsonProperty("repair_failures_time_window")
    public final void setRepairFailuresTimeWindow(final Interval repairFailuresTimeWindow)
    {
        myRepairFailuresTimeWindow = repairFailuresTimeWindow;
    }
    @JsonProperty("trigger_interval_for_metric_inspection")
    public final void setTriggerIntervalForMetricInspection(final Interval triggerIntervalForStatusLogger)
    {
        myTriggerIntervalForMetricInspection = triggerIntervalForStatusLogger;
     }

     public final void validate()
    {
        long repairTimeWindowInSeconds = getRepairFailuresTimeWindow().getInterval(TimeUnit.SECONDS);
        long triggerIntervalForMetricInspection = getTriggerIntervalForMetricInspection()
                .getInterval(TimeUnit.SECONDS);
        if (triggerIntervalForMetricInspection >= repairTimeWindowInSeconds)
        {
            throw new IllegalArgumentException(String.format("""
                            Repair window time must be greater than trigger interval.\
                             Current repair window time: %d seconds,\
                             trigger interval for metric inspection: %d seconds\
                            """,
                    repairTimeWindowInSeconds,
                    triggerIntervalForMetricInspection));
        }
     }

}

