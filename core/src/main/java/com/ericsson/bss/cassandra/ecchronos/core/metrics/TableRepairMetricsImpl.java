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

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

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
    private final String myMetricPrefix;

    private TableRepairMetricsImpl(final Builder builder)
    {
        myMetricRegistry = Preconditions.checkNotNull(builder.myMetricRegistry,
                "Metric registry cannot be null");
        myMetricPrefix = builder.myMetricPrefix;

        myNodeMetricHolder = new NodeMetricHolder(myMetricPrefix, myMetricRegistry,
                Preconditions.checkNotNull(builder.myTableStorageStates,
                        "Table storage states cannot be null"));

        File statisticsDirectory = new File(builder.myStatisticsDirectory);
        if (!statisticsDirectory.exists() && !statisticsDirectory.mkdirs())
        {
            LOG.warn("Failed to create statistics directory: {}, csv files will not be generated",
                    builder.myStatisticsDirectory);
        }

        if (builder.myIsFileReporting)
        {
            myTopLevelCsvReporter = CsvReporter.forRegistry(myMetricRegistry)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .filter(builder.myFileMetricFilter)
                    .build(statisticsDirectory);
            myTopLevelCsvReporter.start(builder.myReportIntervalInMs, builder.myReportIntervalInMs,
                    TimeUnit.MILLISECONDS);
        }
        else
        {
            myTopLevelCsvReporter = null;
        }
        if (builder.myIsJmxReporting)
        {
            myTopLevelJmxReporter = JmxReporter.forRegistry(myMetricRegistry)
                    .filter(builder.myJmxMetricFilter)
                    .build();
            myTopLevelJmxReporter.start();
        }
        else
        {
            myTopLevelJmxReporter = null;
        }
    }

    @Override
    public void repairState(final TableReference tableReference,
                            final int repairedRanges,
                            final int notRepairedRanges)
    {
        tableMetricHolder(tableReference).repairState(repairedRanges, notRepairedRanges);
    }

    @Override
    public Optional<Double> getRepairRatio(final TableReference tableReference)
    {
        return Optional.ofNullable(myNodeMetricHolder.getRepairRatio(tableReference));
    }

    @Override
    public void lastRepairedAt(final TableReference tableReference,
                               final long lastRepairedAt)
    {
        tableMetricHolder(tableReference).lastRepairedAt(lastRepairedAt);
    }

    @Override
    public void remainingRepairTime(final TableReference tableReference,
                                    final long remainingRepairTime)
    {
        tableMetricHolder(tableReference).remainingRepairTime(remainingRepairTime);
    }

    @Override
    public void repairTiming(final TableReference tableReference,
                             final long timeTaken,
                             final TimeUnit timeUnit,
                             final boolean successful)
    {
        tableMetricHolder(tableReference).repairTiming(timeTaken, timeUnit, successful);
    }

    @Override
    public void failedRepairTask(final TableReference tableReference)
    {
        tableMetricHolder(tableReference).failedRepairTask();
    }

    @Override
    public void succeededRepairTask(final TableReference tableReference)
    {
        tableMetricHolder(tableReference).succeededRepairTask();
    }

    @VisibleForTesting
    void report()
    {
        if (myTopLevelCsvReporter != null)
        {
            myTopLevelCsvReporter.report();
        }
    }

    @Override
    public void close()
    {
        if (myTopLevelCsvReporter != null)
        {
            myTopLevelCsvReporter.report();
            myTopLevelCsvReporter.close();
        }

        if (myTopLevelJmxReporter != null)
        {
            myTopLevelJmxReporter.stop();
            myTopLevelJmxReporter.close();
        }

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
        private String myMetricPrefix = "";
        private long myReportIntervalInMs = DEFAULT_STATISTICS_REPORT_INTERVAL_IN_MS;
        private MetricRegistry myMetricRegistry;
        private MetricFilter myJmxMetricFilter = MetricFilter.ALL;
        private MetricFilter myFileMetricFilter = MetricFilter.ALL;
        private boolean myIsJmxReporting = true;
        private boolean myIsFileReporting = true;

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
         * Build with statistics directory.
         *
         * @param statisticsDirectory Statistic directory
         * @return Builder
         */
        public Builder withStatisticsDirectory(final String statisticsDirectory)
        {
            myStatisticsDirectory = statisticsDirectory;
            return this;
        }

        /**
         * Build with report interval.
         *
         * @param reportInterval Report interval
         * @param timeUnit The time unit
         * @return Builder
         */
        public Builder withReportInterval(final long reportInterval, final TimeUnit timeUnit)
        {
            myReportIntervalInMs = timeUnit.toMillis(reportInterval);
            return this;
        }

        /**
         * Build with metric registry.
         *
         * @param metricRegistry Metric registry
         * @return Builder
         */
        public Builder withMetricRegistry(final MetricRegistry metricRegistry)
        {
            myMetricRegistry = metricRegistry;
            return this;
        }

        /**
         * Build with metric filter for jmx reporter.
         *
         * @param metricFilter Metric filter
         * @return Builder
         */
        public Builder withJmxMetricFilter(final MetricFilter metricFilter)
        {
            myJmxMetricFilter = metricFilter;
            return this;
        }

        /**
         * Build with metric filter for file reporter.
         *
         * @param metricFilter Metric filter
         * @return Builder
         */
        public Builder withFileMetricFilter(final MetricFilter metricFilter)
        {
            myFileMetricFilter = metricFilter;
            return this;
        }

        /**
         * Build with JMX reporting.
         *
         * @param isJmxReporting whether to report metrics over JMX
         * @return Builder
         */
        public Builder withJmxReporting(final boolean isJmxReporting)
        {
            myIsJmxReporting = isJmxReporting;
            return this;
        }

        /**
         * Build with file reporting.
         *
         * @param isFileReporting whether to report metrics to a file
         * @return Builder
         */
        public Builder withFileReporting(final boolean isFileReporting)
        {
            myIsFileReporting = isFileReporting;
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
         * Build table repair metrics.
         *
         * @return TableRepairMetricsImpl
         */
        public TableRepairMetricsImpl build()
        {
            return new TableRepairMetricsImpl(this);
        }
    }

    private TableMetricHolder tableMetricHolder(final TableReference tableReference)
    {
        TableMetricHolder tableMetricHolder = myTableMetricHolders.get(tableReference);

        if (tableMetricHolder == null)
        {
            tableMetricHolder = new TableMetricHolder(tableReference, myMetricRegistry, myNodeMetricHolder,
                    myMetricPrefix);

            TableMetricHolder oldTableMetricHolder = myTableMetricHolders.putIfAbsent(tableReference,
                    tableMetricHolder);

            if (oldTableMetricHolder != null)
            {
                tableMetricHolder = oldTableMetricHolder;
            }

            tableMetricHolder.init();
        }

        return tableMetricHolder;
    }
}
