
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
package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import com.ericsson.bss.cassandra.ecchronos.core.utils.StatusLogger;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;

public final class MetricInspector
{

    private final MeterRegistry myMeterRegistry;
    private long myRepairFailureCountSinceLastReport = 0;
    private static final long REPEAT_INTERVAL_PERIOD_IN_MILLISECONDS = 5000;
    private static final long  DEFAULT_TIME_WINDOW = 30;
    private static final int DEFAULT_REPAIR_FAILURES_THRESHOLD = 5;
    private long myTotalRecordFailures = 0;
    private int myRepairFailureThreshold = DEFAULT_REPAIR_FAILURES_THRESHOLD;
    private long myRepairFailureTimeWindow = DEFAULT_TIME_WINDOW;
    private long myTriggerIntervalForMetricInspection = REPEAT_INTERVAL_PERIOD_IN_MILLISECONDS;
    private LocalDateTime myRecordingStartTimestamp = LocalDateTime.now();
    private Timer timer;

    @VisibleForTesting
    long getRepairFailuresCountSinceLastReport()
     {
        return myRepairFailureCountSinceLastReport;
    }

    @VisibleForTesting
    long getTotalRecordFailures()
    {
        return myTotalRecordFailures;
    }

    @VisibleForTesting
    LocalDateTime getRecordingStartTimestamp()
    {
        return myRecordingStartTimestamp;
    }

    public MetricInspector(final MeterRegistry meterRegistry,
                           final int repairFailureThreshold,
                           final long repairFailureTimeWindow)
    {
        this.myMeterRegistry = meterRegistry;
        this.myRepairFailureThreshold = repairFailureThreshold;
        this.myRepairFailureTimeWindow = repairFailureTimeWindow;
    }

    public MetricInspector(final MeterRegistry meterRegistry,
                    final int repairFailureThreshold,
                    final long repairFailureTimeWindow,
                    final long triggerIntervalForMetricInspection)
    {
        this.myMeterRegistry = meterRegistry;
        this.myRepairFailureThreshold = repairFailureThreshold;
        this.myRepairFailureTimeWindow = repairFailureTimeWindow;
        this.myTriggerIntervalForMetricInspection = triggerIntervalForMetricInspection;
    }

    public void startInspection()
    {
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask()
        {
            @Override
            public void run()
            {
                inspectMeterRegistryForRepairFailures();
            }
        }, 0, REPEAT_INTERVAL_PERIOD_IN_MILLISECONDS);
    }

    public void stopInspection()
    {
        if (timer != null)
        {
            timer.cancel();
        }
    }

    @VisibleForTesting
    void inspectMeterRegistryForRepairFailures()
    {
        io.micrometer.core.instrument.Timer nodeRepairSessions = myMeterRegistry
                     .find(TableRepairMetricsImpl.NODE_REPAIR_SESSIONS)
                    .tags("successful", "false")
                    .timer();
        if (nodeRepairSessions != null)
            {
                myTotalRecordFailures = nodeRepairSessions.count();
            }
        if (myTotalRecordFailures - myRepairFailureCountSinceLastReport > myRepairFailureThreshold)
            {
                //reset count failure and reinitialize time window
                myRepairFailureCountSinceLastReport = myTotalRecordFailures;
                myRecordingStartTimestamp = LocalDateTime.now();
                StatusLogger.log(myMeterRegistry);
            }
        resetRepairFailureCount();
    }

    /*
     * If in defined time window, number of repair failure has not crossed the configured number, then
     * reset failure count for new timed window. Reinitialize time window.
     */

    @VisibleForTesting
    void resetRepairFailureCount()
        {
            LocalDateTime currentTimeStamp = LocalDateTime.now();
            LocalDateTime timeRepairWindowMinutesAgo = currentTimeStamp.
                    minus(myRepairFailureTimeWindow, ChronoUnit.MINUTES);
            if (myRecordingStartTimestamp.isBefore(timeRepairWindowMinutesAgo))
            {
                myRepairFailureCountSinceLastReport = myTotalRecordFailures;
                myRecordingStartTimestamp = LocalDateTime.now();
            }
        };
}
