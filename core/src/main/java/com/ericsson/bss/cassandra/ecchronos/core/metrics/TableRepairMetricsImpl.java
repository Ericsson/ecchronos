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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
    static final String TIME_SINCE_LAST_REPAIRED = "time.since.last.repaired";
    static final String REMAINING_REPAIR_TIME = "remaining.repair.time";
    static final String REPAIR_SESSIONS = "repair.sessions";

    @VisibleForTesting
    interface Clock
    {
        long timeNow();
    }

    @VisibleForTesting
    static Clock clock = () -> System.currentTimeMillis();


    private final String myRepairedRatioMetricName;
    private final String myTimeSinceLastRepairedMetricName;
    private final String myRemainingRepairTimeMetricName;
    private final String myRepairSessionsMetricName;

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
            myTimeSinceLastRepairedMetricName = builder.myMetricPrefix + "." + TIME_SINCE_LAST_REPAIRED;
            myRemainingRepairTimeMetricName = builder.myMetricPrefix + "." + REMAINING_REPAIR_TIME;
            myRepairSessionsMetricName = builder.myMetricPrefix + "." + REPAIR_SESSIONS;
        }
        else
        {
            myRepairedRatioMetricName = REPAIRED_RATIO;
            myTimeSinceLastRepairedMetricName = TIME_SINCE_LAST_REPAIRED;
            myRemainingRepairTimeMetricName = REMAINING_REPAIR_TIME;
            myRepairSessionsMetricName = REPAIR_SESSIONS;
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
        TimeGauge.builder(myTimeSinceLastRepairedMetricName, myTableGauges, TimeUnit.MILLISECONDS,
                        (tableGauges) -> clock.timeNow() - tableGauges.get(tableReference).getLastRepairedAt())
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
    public void repairSession(final TableReference tableReference,
                              final long timeTaken,
                              final TimeUnit timeUnit,
                              final boolean successful)
    {
        Timer.builder(myRepairSessionsMetricName)
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(),
                      TABLE_TAG, tableReference.getTable(),
                      "successful", Boolean.toString(successful))
                .register(myMeterRegistry)
                .record(timeTaken, timeUnit);
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
