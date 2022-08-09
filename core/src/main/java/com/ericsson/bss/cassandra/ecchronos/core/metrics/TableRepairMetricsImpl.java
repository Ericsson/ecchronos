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
import java.io.File;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.jmx.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public final class TableRepairMetricsImpl implements TableRepairMetrics, TableRepairMetricsProvider, Closeable
{
	private static final Logger LOG = LoggerFactory.getLogger(TableRepairMetricsImpl.class);

    private static final String DEFAULT_STATISTICS_DIRECTORY = "/var/lib/cassandra/repair/metrics/";
    private static final long DEFAULT_STATISTICS_REPORT_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(60);

    private final CsvReporter myTopLevelCsvReporter;
    private final JmxReporter myTopLevelJmxReporter;

    private final ConcurrentHashMap<TableReference, TableMetricHolder> myTableMetricHolders = new ConcurrentHashMap<>();

    private final MetricRegistry myMetricRegistry;
    private final NodeMetricHolder myNodeMetricHolder;

    private TableRepairMetricsImpl(Builder builder)
    {
        myMetricRegistry = Preconditions.checkNotNull(builder.myMetricRegistry, "Metric registry cannot be null");

        myNodeMetricHolder = new NodeMetricHolder(myMetricRegistry,
                Preconditions.checkNotNull(builder.myTableStorageStates, "Table storage states cannot be null"));

        File statisticsDirectory = new File(builder.myStatisticsDirectory);
        if(!statisticsDirectory.exists() && !statisticsDirectory.mkdirs())
        {
            LOG.warn("Failed to create statistics directory: {}, csv files will not be generated", builder.myStatisticsDirectory);
        }

        myTopLevelCsvReporter = CsvReporter.forRegistry(myMetricRegistry)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .build(statisticsDirectory);
        myTopLevelJmxReporter = JmxReporter.forRegistry(myMetricRegistry)
                .build();

        myTopLevelCsvReporter.start(builder.myReportIntervalInMs, builder.myReportIntervalInMs, TimeUnit.MILLISECONDS);
        myTopLevelJmxReporter.start();
    }

    @Override
    public void repairState(TableReference tableReference, int repairedRanges, int notRepairedRanges)
    {
        tableMetricHolder(tableReference).repairState(repairedRanges, notRepairedRanges);
    }

    @Override
    public Optional<Double> getRepairRatio(TableReference tableReference)
    {
        return Optional.ofNullable(myNodeMetricHolder.getRepairRatio(tableReference));
    }

    @Override
    public void lastRepairedAt(TableReference tableReference, long lastRepairedAt)
    {
        tableMetricHolder(tableReference).lastRepairedAt(lastRepairedAt);
    }

    @Override
    public void remainingRepairTime(TableReference tableReference, long remainingRepairTime)
    {
        tableMetricHolder(tableReference).remainingRepairTime(remainingRepairTime);
    }

    @Override
    public void repairTiming(TableReference tableReference, long timeTaken, TimeUnit timeUnit, boolean successful)
    {
        tableMetricHolder(tableReference).repairTiming(timeTaken, timeUnit, successful);
    }

    @VisibleForTesting
    void report()
    {
        myTopLevelCsvReporter.report();
    }

    @Override
    public void close()
    {
        myTopLevelCsvReporter.report();
        myTopLevelCsvReporter.close();

        myTopLevelJmxReporter.stop();
        myTopLevelJmxReporter.close();

        myNodeMetricHolder.close();

        for (TableMetricHolder tableMetricHolder : myTableMetricHolders.values())
        {
            tableMetricHolder.close();
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private TableStorageStates myTableStorageStates;
        private String myStatisticsDirectory = DEFAULT_STATISTICS_DIRECTORY;
        private long myReportIntervalInMs = DEFAULT_STATISTICS_REPORT_INTERVAL_IN_MS;
        private MetricRegistry myMetricRegistry;

        public Builder withTableStorageStates(TableStorageStates tableStorageStates)
        {
            myTableStorageStates = tableStorageStates;
            return this;
        }

        public Builder withStatisticsDirectory(String statisticsDirectory)
        {
            myStatisticsDirectory = statisticsDirectory;
            return this;
        }

        public Builder withReportInterval(long reportInterval, TimeUnit timeUnit)
        {
            myReportIntervalInMs = timeUnit.toMillis(reportInterval);
            return this;
        }

        public Builder withMetricRegistry(MetricRegistry metricRegistry)
        {
            myMetricRegistry = metricRegistry;
            return this;
        }

        public TableRepairMetricsImpl build()
        {
            return new TableRepairMetricsImpl(this);
        }
    }

    private TableMetricHolder tableMetricHolder(TableReference tableReference)
    {
        TableMetricHolder tableMetricHolder = myTableMetricHolders.get(tableReference);

        if (tableMetricHolder == null)
        {
            tableMetricHolder = new TableMetricHolder(tableReference, myMetricRegistry, myNodeMetricHolder);

            TableMetricHolder oldTableMetricHolder = myTableMetricHolders.putIfAbsent(tableReference, tableMetricHolder);

            if (oldTableMetricHolder != null)
            {
                tableMetricHolder = oldTableMetricHolder;
            }

            tableMetricHolder.init();
        }

        return tableMetricHolder;
    }
}
