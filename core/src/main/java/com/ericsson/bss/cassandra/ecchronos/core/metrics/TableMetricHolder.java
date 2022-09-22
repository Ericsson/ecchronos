/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.SlidingTimeWindowMovingAverages;
import com.codahale.metrics.Timer;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * Holder class for repair metrics related to a specific table.
 *
 * This holder will update the {@link NodeMetricHolder} when updated.
 */
public class TableMetricHolder implements Closeable
{
    static final String REPAIR_TIMING_SUCCESS = "RepairSuccessTime";
    static final String REPAIR_TIMING_FAILED = "RepairFailedTime";
    static final String LAST_REPAIRED_AT = "LastRepairedAt";
    static final String REPAIR_STATE = "RepairState";
    static final String REMAINING_REPAIR_TIME = "RemainingRepairTime";
    static final String FAILED_REPAIR_TASKS = "FailedRepairTasks";
    static final String SUCCEEDED_REPAIR_TASKS = "SucceededRepairTasks";

    private final MetricRegistry myMetricRegistry;
    private final NodeMetricHolder myNodeMetricHolder;

    private final TableReference myTableReference;

    private final AtomicReference<RangeRepairState> myRepairState = new AtomicReference<>();
    private final AtomicReference<Long> myLastRepairedAt = new AtomicReference<>(0L);
    private final AtomicReference<Long> myRemainingRepairTime = new AtomicReference<>(0L);
    private final AtomicReference<Meter> myRepairFailedAttempts = new AtomicReference<>(null);
    private final AtomicReference<Meter> myRepairSucceededAttempts = new AtomicReference<>(null);

    public TableMetricHolder(final TableReference tableReference,
                             final MetricRegistry metricRegistry,
                             final NodeMetricHolder nodeMetricHolder)
    {
        myTableReference = tableReference;
        myMetricRegistry = metricRegistry;
        myNodeMetricHolder = nodeMetricHolder;
    }

    /**
     * Initialize all metrics and register them in metric registry.
     */
    public void init()
    {
        myMetricRegistry.register(metricName(REPAIR_STATE), new RatioGauge()
        {
            @Override
            public Ratio getRatio()
            {
                RangeRepairState repairState = myRepairState.get();

                if (repairState != null)
                {
                    return Ratio.of(repairState.getRepairedRanges(), repairState.getFullRanges());
                }

                return Ratio.of(0, 0);
            }
        });
        myMetricRegistry.register(metricName(LAST_REPAIRED_AT), lastRepairedAtGauge());
        myMetricRegistry.register(metricName(REMAINING_REPAIR_TIME), remainingRepairTimeGauge());
        SlidingTimeWindowMovingAverages failedAverages = new SlidingTimeWindowMovingAverages();
        Meter failedAttemptsMeter = new Meter(failedAverages);
        myRepairFailedAttempts.set(failedAttemptsMeter);
        myMetricRegistry.register(metricName(FAILED_REPAIR_TASKS), myRepairFailedAttempts.get());
        SlidingTimeWindowMovingAverages succeededAverages = new SlidingTimeWindowMovingAverages();
        Meter succeededAttemptsMeter = new Meter(succeededAverages);
        myRepairSucceededAttempts.set(succeededAttemptsMeter);
        myMetricRegistry.register(metricName(SUCCEEDED_REPAIR_TASKS), myRepairSucceededAttempts.get());
        timer(REPAIR_TIMING_SUCCESS);
        timer(REPAIR_TIMING_FAILED);
    }

    /**
     * Update repair state metric.
     *
     * @param repairedRanges Ranges repaired
     * @param notRepairedRanges Ranges NOT repaired
     */
    public void repairState(final int repairedRanges, final int notRepairedRanges)
    {
        myRepairState.set(new RangeRepairState(repairedRanges, notRepairedRanges));

        double ratio;

        if (notRepairedRanges == 0)
        {
            ratio = 1;
        }
        else if (repairedRanges == 0)
        {
            ratio = 0;
        }
        else
        {
            ratio = (double) repairedRanges / (repairedRanges + notRepairedRanges);
        }

        myNodeMetricHolder.repairState(myTableReference, ratio);
    }

    /**
     * Update last repaired at metric.
     *
     * @param lastRepairedAt Last repaired at
     */
    public void lastRepairedAt(final long lastRepairedAt)
    {
        myLastRepairedAt.set(lastRepairedAt);
    }

    /**
     * Update remaining repair time metric.
     *
     * @param remainingRepairTime Remaining repair time
     */
    public void remainingRepairTime(final long remainingRepairTime)
    {
        myRemainingRepairTime.set(remainingRepairTime);
    }

    /**
     * Update failed repair time metric.
     */
    public void failedRepairTask()
    {
        myRepairFailedAttempts.get().mark();
    }

    /**
     * Increment succeeded repair tasks metric.
     */
    public void succeededRepairTask()
    {
        myRepairSucceededAttempts.get().mark();
    }

    /**
     * Update repair timing metric.
     *
     * @param timeTaken Time taken
     * @param timeUnit Time unit
     * @param successful Successful flag
     */
    public void repairTiming(final long timeTaken, final TimeUnit timeUnit, final boolean successful)
    {
        if (successful)
        {
            timer(REPAIR_TIMING_SUCCESS).update(timeTaken, timeUnit);
        }
        else
        {
            timer(REPAIR_TIMING_FAILED).update(timeTaken, timeUnit);
        }

        myNodeMetricHolder.repairTiming(timeTaken, timeUnit, successful);
    }

    /**
     * Removes metrics from the registry when closing the JMX connection.
     */
    @Override
    public void close()
    {
        myMetricRegistry.remove(metricName(REPAIR_TIMING_SUCCESS));
        myMetricRegistry.remove(metricName(REPAIR_TIMING_FAILED));
        myMetricRegistry.remove(metricName(LAST_REPAIRED_AT));
        myMetricRegistry.remove(metricName(REPAIR_STATE));
        myMetricRegistry.remove(metricName(REMAINING_REPAIR_TIME));
        myMetricRegistry.remove(metricName(FAILED_REPAIR_TASKS));
        myMetricRegistry.remove(metricName(SUCCEEDED_REPAIR_TASKS));
    }

    private String metricName(final String name)
    {
        return myTableReference.getKeyspace() + "." + myTableReference.getTable() + "-" + myTableReference.getId() + "-"
                + name;
    }

    private Timer timer(final String name)
    {
        return myMetricRegistry.timer(metricName(name), Timer::new);
    }

    private Gauge<Long> lastRepairedAtGauge()
    {
        return myLastRepairedAt::get;
    }

    private Gauge<Long> remainingRepairTimeGauge()
    {
        return myRemainingRepairTime::get;
    }

    private static class RangeRepairState
    {
        private final int myRepairedRanges;
        private final int myFullRanges;

        RangeRepairState(final int repairedRanges, final int notRepairedRanges)
        {
            myRepairedRanges = repairedRanges;
            myFullRanges = repairedRanges + notRepairedRanges;
        }

        public int getRepairedRanges()
        {
            return myRepairedRanges;
        }

        public int getFullRanges()
        {
            return myFullRanges;
        }
    }
}
