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
import java.util.Map;
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
    static final String NODE_REPAIRED_RATIO = "node.repaired.ratio";
    static final String NODE_TIME_SINCE_LAST_REPAIRED = "node.time.since.last.repaired";
    static final String NODE_REMAINING_REPAIR_TIME = "node.remaining.repair.time";
    static final String NODE_REPAIR_SESSIONS = "node.repair.sessions";

    @VisibleForTesting
    @FunctionalInterface
    interface Clock
    {
        long timeNow();
    }

    @VisibleForTesting
    static Clock clock = () -> System.currentTimeMillis(); // NOPMD

    private final Map<TableReference, TableGauges> myTableGauges = new ConcurrentHashMap<>();
    private final MeterRegistry myMeterRegistry;

    private TableRepairMetricsImpl(final Builder builder)
    {
        myMeterRegistry = Preconditions.checkNotNull(builder.myMeterRegistry, "Meter registry cannot be null");
    }

    @Override
    public void repairState(final TableReference tableReference,
                            final int repairedRanges,
                            final int notRepairedRanges)
    {
        createOrGetTableGauges(tableReference).repairRatio(repairedRanges, notRepairedRanges);
        Gauge.builder(REPAIRED_RATIO, myTableGauges,
                        (tableGauges) -> tableGauges.get(tableReference).getRepairRatio())
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(), TABLE_TAG, tableReference.getTable())
                .register(myMeterRegistry);
        Gauge.builder(NODE_REPAIRED_RATIO, myTableGauges,
                        (tableGauges) -> tableGauges.values().stream().mapToDouble(TableGauges::getRepairRatio)
                                .average().orElse(0))
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
        TimeGauge.builder(TIME_SINCE_LAST_REPAIRED, myTableGauges, TimeUnit.MILLISECONDS,
                        (tableGauges) -> clock.timeNow() - tableGauges.get(tableReference).getLastRepairedAt())
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(), TABLE_TAG, tableReference.getTable())
                .register(myMeterRegistry);
        TimeGauge.builder(NODE_TIME_SINCE_LAST_REPAIRED, myTableGauges, TimeUnit.MILLISECONDS,
                        (tableGauges) ->
                                clock.timeNow() - tableGauges.values().stream()
                                        .mapToDouble(TableGauges::getLastRepairedAt).min().orElse(0))
                .register(myMeterRegistry);
    }

    @Override
    public void remainingRepairTime(final TableReference tableReference,
                                    final long remainingRepairTime)
    {
        createOrGetTableGauges(tableReference).remainingRepairTime(remainingRepairTime);
        TimeGauge.builder(REMAINING_REPAIR_TIME, myTableGauges, TimeUnit.MILLISECONDS,
                        (tableGauges) -> tableGauges.get(tableReference).getRemainingRepairTime())
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(), TABLE_TAG, tableReference.getTable())
                .register(myMeterRegistry);
        TimeGauge.builder(NODE_REMAINING_REPAIR_TIME, myTableGauges, TimeUnit.MILLISECONDS,
                        (tableGauges) ->
                                tableGauges.values().stream().mapToDouble(TableGauges::getRemainingRepairTime).sum())
                .register(myMeterRegistry);
    }

    @Override
    public void repairSession(final TableReference tableReference,
                              final long timeTaken,
                              final TimeUnit timeUnit,
                              final boolean successful)
    {
        Timer.builder(REPAIR_SESSIONS)
                .tags(KEYSPACE_TAG, tableReference.getKeyspace(),
                      TABLE_TAG, tableReference.getTable(),
                      "successful", Boolean.toString(successful))
                .register(myMeterRegistry)
                .record(timeTaken, timeUnit);
        Timer.builder(NODE_REPAIR_SESSIONS)
                .tags("successful", Boolean.toString(successful))
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
        private MeterRegistry myMeterRegistry;

        /**
         * Build with table storage states.
         *
         * @deprecated
         * This is not used anymore, calling this method is a NoOP
         *
         * @param tableStorageStates Table storage states
         * @return Builder
         */
        @Deprecated
        public Builder withTableStorageStates(final TableStorageStates tableStorageStates)
        {
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
