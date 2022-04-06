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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
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

    private final MetricRegistry myMetricRegistry;
    private final NodeMetricHolder myNodeMetricHolder;

    private final TableReference myTableReference;

    private final AtomicReference<RangeRepairState> myRepairState = new AtomicReference<>();
    private final AtomicReference<Long> myLastRepairedAt = new AtomicReference<>(0L);
    private final AtomicReference<Long> myRemainingRepairTime = new AtomicReference<>(0L);;

    public TableMetricHolder(TableReference tableReference, MetricRegistry metricRegistry, NodeMetricHolder nodeMetricHolder)
    {
        myTableReference = tableReference;
        myMetricRegistry = metricRegistry;
        myNodeMetricHolder = nodeMetricHolder;
    }

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
        timer(REPAIR_TIMING_SUCCESS);
        timer(REPAIR_TIMING_FAILED);
    }

    public void repairState(int repairedRanges, int notRepairedRanges)
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
            ratio = (double)repairedRanges / (repairedRanges + notRepairedRanges);
        }

        myNodeMetricHolder.repairState(myTableReference, ratio);
    }

    public void lastRepairedAt(long lastRepairedAt)
    {
        myLastRepairedAt.set(lastRepairedAt);
    }

    public void remainingRepairTime(long remainingRepairTime)
    {
        myRemainingRepairTime.set(remainingRepairTime);
    }

    public void repairTiming(long timeTaken, TimeUnit timeUnit, boolean successful)
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

    @Override
    public void close()
    {
        myMetricRegistry.remove(metricName(REPAIR_TIMING_SUCCESS));
        myMetricRegistry.remove(metricName(REPAIR_TIMING_FAILED));
        myMetricRegistry.remove(metricName(LAST_REPAIRED_AT));
        myMetricRegistry.remove(metricName(REPAIR_STATE));
        myMetricRegistry.remove(metricName(REMAINING_REPAIR_TIME));
    }

    private String metricName(String name)
    {
        return myTableReference.getKeyspace() + "." + myTableReference.getTable() + "-" + myTableReference.getId() + "-"
                + name;
    }

    private Timer timer(String name)
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

        public RangeRepairState(int repairedRanges, int notRepairedRanges)
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
