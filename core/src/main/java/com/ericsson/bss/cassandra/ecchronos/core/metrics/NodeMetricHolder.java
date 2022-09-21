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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * A holder class for repair metrics related to this node.
 *
 * The top-level holder is an aggregated view of the different {@link TableMetricHolder}
 */
public class NodeMetricHolder implements Closeable
{
    static final String REPAIRED_TABLES = "TableRepairState";
    static final String REPAIRED_DATA = "DataRepairState";
    static final String REPAIR_TIMING_SUCCESS = "RepairSuccessTime";
    static final String REPAIR_TIMING_FAILED = "RepairFailedTime";

    private final ConcurrentHashMap<TableReference, Double> myTableRepairRatio = new ConcurrentHashMap<>();

    private final MetricRegistry myMetricRegistry;

    public NodeMetricHolder(final MetricRegistry metricRegistry, final TableStorageStates tableStorageStates)
    {
        myMetricRegistry = metricRegistry;

        // Initialize metrics
        timer(REPAIR_TIMING_SUCCESS);
        timer(REPAIR_TIMING_FAILED);

        myMetricRegistry.gauge(REPAIRED_TABLES, () -> new RatioGauge()
        {
            @Override
            protected Ratio getRatio()
            {
                double averageRatio = myTableRepairRatio.values().stream()
                        .mapToDouble(d -> d)
                        .average()
                        .orElse(1); // 100% when no tables to repair

                return Ratio.of(averageRatio, 1);
            }
        });

        myMetricRegistry.gauge(REPAIRED_DATA, () -> new RatioGauge()
        {
            @Override
            protected Ratio getRatio()
            {
                long totalDataSize = tableStorageStates.getDataSize();
                if (totalDataSize == 0)
                {
                    return Ratio.of(1, 1); // 100% when no data to repair
                }

                double repairedDataSize = myTableRepairRatio.entrySet().stream()
                        .mapToDouble(e -> e.getValue() * tableStorageStates.getDataSize(e.getKey()))
                        .sum();

                return Ratio.of(repairedDataSize, totalDataSize);
            }
        });
    }

    /**
     * Get repair state.
     *
     * @param tableReference Table reference
     * @param repairRatio The repair ratio
     */
    public void repairState(final TableReference tableReference, final double repairRatio)
    {
        myTableRepairRatio.put(tableReference, repairRatio);
    }

    /**
     * Get repair ratio.
     *
     * @param tableReference Table reference
     * @return Double
     */
    public Double getRepairRatio(final TableReference tableReference)
    {
        return myTableRepairRatio.get(tableReference);
    }

    /**
     * Repair timing.
     *
     * @param timeTaken Time token
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
    }

    private Timer timer(final String name)
    {
        return myMetricRegistry.timer(name, Timer::new);
    }

    /**
     * Close.
     */
    @Override
    public void close()
    {
        myMetricRegistry.remove(REPAIR_TIMING_SUCCESS);
        myMetricRegistry.remove(REPAIR_TIMING_FAILED);
        myMetricRegistry.remove(REPAIRED_TABLES);
        myMetricRegistry.remove(REPAIRED_DATA);
    }
}
