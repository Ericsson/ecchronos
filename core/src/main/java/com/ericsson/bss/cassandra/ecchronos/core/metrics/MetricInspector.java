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
    private static final int REPEAT_INTERVAL_PERIOD_IN_MILLISECONDS = 5000;
    private long myTotalRecordFailures = 0;
    private final int myRepairFailureThreshold;
    private final int myRepairFailureTimeWindow;
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
                           final int repairFailureTimeWindow)
    {
        this.myMeterRegistry = meterRegistry;
        this.myRepairFailureThreshold = repairFailureThreshold;
        this.myRepairFailureTimeWindow = repairFailureTimeWindow;
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
