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

import com.ericsson.bss.cassandra.ecchronos.application.config.Interval;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;
import java.util.concurrent.TimeUnit;

/** Configuration for metrics and statistics collection. */
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

    /** Default constructor. */
    public StatisticsConfig()
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
     * Returns the output directory.
     * @return the output directory
     */
    @JsonProperty("directory")
    public final File getOutputDirectory()
    {
        return myOutputDirectory;
    }

    /**
     * Returns the reporting configs.
     * @return the reporting configs
     */
    @JsonProperty("reporting")
    public final ReportingConfigs getReportingConfigs()
    {
        return myReportingConfigs;
    }

    /**
     * Returns the metrics prefix.
     * @return the metrics prefix
     */
    @JsonProperty("prefix")
    public final String getMetricsPrefix()
    {
        return myMetricsPrefix;
    }

    /**
     * Returns the repair failures count.
     * @return the repair failures count
     */
    @JsonProperty("repair_failures_count")
    public final int getRepairFailuresCount()
    {
        return myRepairFailuresCount;
    }

    /**
     * Returns the repair failures time window.
     * @return the repair failures time window
     */
    @JsonProperty("repair_failures_time_window")
    public final Interval getRepairFailuresTimeWindow()
    {
        return myRepairFailuresTimeWindow;
    }

    /**
     * Returns the trigger interval for metric inspection.
     * @return the trigger interval for metric inspection
     */
    @JsonProperty("trigger_interval_for_metric_inspection")
    public final Interval getTriggerIntervalForMetricInspection()
    {
        return myTriggerIntervalForMetricInspection;
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
     * Sets the output directory.
     * @param outputDirectory the output directory
     */
    @JsonProperty("directory")
    public final void setOutputDirectory(final String outputDirectory)
    {
        myOutputDirectory = new File(outputDirectory);
    }

    /**
     * Sets the reporting configs.
     * @param reportingConfigs the reporting configs
     */
    @JsonProperty("reporting")
    public final void setReportingConfigs(final ReportingConfigs reportingConfigs)
    {
        myReportingConfigs = reportingConfigs;
    }

    /**
     * Sets the metrics prefix.
     * @param metricsPrefix the metrics prefix
     */
    @JsonProperty("prefix")
    public final void setMetricsPrefix(final String metricsPrefix)
    {
        myMetricsPrefix = metricsPrefix;
    }

    /**
     * Sets the repair failures count.
     * @param repairFailuresCount the repair failures count
     */
    @JsonProperty("repair_failures_count")
    public final void setRepairFailuresCount(final int repairFailuresCount)
    {
        myRepairFailuresCount = repairFailuresCount;
    }

    /**
     * Sets the repair failures time window.
     * @param repairFailuresTimeWindow the repair failures time window
     */
    @JsonProperty("repair_failures_time_window")
    public final void setRepairFailuresTimeWindow(final Interval repairFailuresTimeWindow)
    {
        myRepairFailuresTimeWindow = repairFailuresTimeWindow;
    }
    /**
     * Sets the trigger interval for metric inspection.
     * @param triggerIntervalForStatusLogger the trigger interval for status logger
     */
    @JsonProperty("trigger_interval_for_metric_inspection")
    public final void setTriggerIntervalForMetricInspection(final Interval triggerIntervalForStatusLogger)
    {
        myTriggerIntervalForMetricInspection = triggerIntervalForStatusLogger;
     }

     /** Validates the configuration. */
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

