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

import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class TableRepairMetricsImpl implements TableRepairMetrics, TableRepairMetricsProvider, Closeable
{
    private static final String KEYSPACE_TAG = "keyspace";
    private static final String TABLE_TAG = "table";

    static final String REPAIRED_RATIO = "repaired.ratio";
    static final String LAST_REPAIRED_AT = "last.repaired.at";
    static final String REMAINING_REPAIR_TIME = "remaining.repair.time";
    static final String REPAIR_TIME_TAKEN = "repair.time.taken";
    static final String REPAIR_TASKS_RUN = "repair.tasks.run";

    private final String myRepairedRatioMetricName;
    private final String myLastRepairedAtMetricName;
    private final String myRemainingRepairTimeMetricName;
    private final String myRepairTimeTakenMetricName;
    private final String myRepairTasksRunMetricName;

    private final ConcurrentHashMap<TableReference, TableGauges> myTableGauges = new ConcurrentHashMap<>();
    private final TableStorageStates myTableStorageStates;
    private final MeterRegistry myMeterRegistry;

    private TableRepairMetricsImpl(final Builder builder)
    {
        myTableStorageStates = Preconditions.checkNotNull(builder.myTableStorageStates,
                "Table storage states cannot be null");
        myMeterRegistry = Preconditions.checkNotNull(builder.myMeterRegistry, "Meter registry cannot be null");
        if (builder.myMetricPrefix != null && !builder.myMetricPrefix.isEmpty())
        {
            myRepairedRatioMetricName = builder.myMetricPrefix + "." + REPAIRED_RATIO;
            myLastRepairedAtMetricName = builder.myMetricPrefix + "." + LAST_REPAIRED_AT;
            myRemainingRepairTimeMetricName = builder.myMetricPrefix + "." + REMAINING_REPAIR_TIME;
            myRepairTimeTakenMetricName = builder.myMetricPrefix + "." + REPAIR_TIME_TAKEN;
            myRepairTasksRunMetricName = builder.myMetricPrefix + "." + REPAIR_TASKS_RUN;
        }
        else
        {
            myRepairedRatioMetricName = REPAIRED_RATIO;
            myLastRepairedAtMetricName = LAST_REPAIRED_AT;
            myRemainingRepairTimeMetricName = REMAINING_REPAIR_TIME;
            myRepairTimeTakenMetricName = REPAIR_TIME_TAKEN;
            myRepairTasksRunMetricName = REPAIR_TASKS_RUN;
        }
    }

    @Override
    public void repairState(final TableReference tableReference,
                            final int repairedRanges,
                            final int notRepairedRanges)
    {
        createOrGetTableGauges(tableReference).repairRatio(repairedRanges, notRepairedRanges);
        Gauge.builder(myRepairedRatioMetricName, myTableGauges,
                        (tableGauges) -> tableGauges.get(tableReference).getRepairRatio())
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(), TABLE_TAG, tableReference.getTable())
                .register(myMeterRegistry);
    }

    @Override
    public Optional<Double> getRepairRatio(final TableReference tableReference)
    {
        if (myTableGauges.get(tableReference) == null)
        {
            return Optional.empty();
        }
        return Optional.ofNullable(myTableGauges.get(tableReference).getRepairRatio());
    }

    @Override
    public void lastRepairedAt(final TableReference tableReference,
                               final long lastRepairedAt)
    {
        createOrGetTableGauges(tableReference).lastRepairedAt(lastRepairedAt);
        TimeGauge.builder(myLastRepairedAtMetricName, myTableGauges, TimeUnit.MILLISECONDS,
                        (tableGauges) -> tableGauges.get(tableReference).getLastRepairedAt())
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(), TABLE_TAG, tableReference.getTable())
                .register(myMeterRegistry);
    }

    @Override
    public void remainingRepairTime(final TableReference tableReference,
                                    final long remainingRepairTime)
    {
        createOrGetTableGauges(tableReference).remainingRepairTime(remainingRepairTime);
        TimeGauge.builder(myRemainingRepairTimeMetricName, myTableGauges, TimeUnit.MILLISECONDS,
                        (tableGauges) -> tableGauges.get(tableReference).getRemainingRepairTime())
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(), TABLE_TAG, tableReference.getTable())
                .register(myMeterRegistry);
    }

    @Override
    public void repairTiming(final TableReference tableReference,
                             final long timeTaken,
                             final TimeUnit timeUnit,
                             final boolean successful)
    {
        Timer.builder(myRepairTimeTakenMetricName)
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(),
                      TABLE_TAG, tableReference.getTable(),
                      "successful", Boolean.toString(successful))
                .register(myMeterRegistry)
                .record(timeTaken, timeUnit);
    }

    @Override
    public void failedRepairTask(final TableReference tableReference)
    {
        Counter.builder(myRepairTasksRunMetricName)
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(),
                        TABLE_TAG, tableReference.getTable(),
                        "successful", Boolean.toString(false))
                .register(myMeterRegistry).increment();
    }

    @Override
    public void succeededRepairTask(final TableReference tableReference)
    {
        Counter.builder(myRepairTasksRunMetricName)
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(),
                      TABLE_TAG, tableReference.getTable(),
                      "successful", Boolean.toString(true))
                .register(myMeterRegistry).increment();
    }

    @Override
    public void close()
    {
        for (TableGauges tableGauges : myTableGauges.values())
        {
            tableGauges.close();
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private TableStorageStates myTableStorageStates;
        private String myMetricPrefix = "";
        private MeterRegistry myMeterRegistry;

        /**
         * Build with table storage states.
         *
         * @param tableStorageStates Table storage states
         * @return Builder
         */
        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        /**
         * Build with metric prefix.
         *
         * @param metricPrefix prefix to prepend all metrics with
         * @return Builder
         */
        public Builder withMetricPrefix(final String metricPrefix)
        {
            myMetricPrefix = metricPrefix;
            return this;
        }

        /**
         * Build with meter registry.
         *
         * @param meterRegistry meter registry to register all metrics towards
         * @return Builder
         */
        public Builder withMeterRegistry(final MeterRegistry meterRegistry)
        {
            myMeterRegistry = meterRegistry;
            return this;
        }

        /**
         * Build table repair metrics.
         *
         * @return TableRepairMetricsImpl
         */
        public TableRepairMetricsImpl build()
        {
            return new TableRepairMetricsImpl(this);
        }
    }

    private TableGauges createOrGetTableGauges(final TableReference tableReference)
    {
        TableGauges tableGauges = myTableGauges.get(tableReference);

        if (tableGauges == null)
        {
            tableGauges = new TableGauges();

            TableGauges oldTableGauges = myTableGauges.putIfAbsent(tableReference,
                    tableGauges);

            if (oldTableGauges != null)
            {
                tableGauges = oldTableGauges;
            }
        }

        return tableGauges;
    }
}
