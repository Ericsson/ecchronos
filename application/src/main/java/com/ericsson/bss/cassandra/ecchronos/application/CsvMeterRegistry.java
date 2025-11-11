/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.application;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CsvMeterRegistry extends DropwizardMeterRegistry
{
    private static final Logger LOG = LoggerFactory.getLogger(CsvMeterRegistry.class);
    private static final long DEFAULT_STATISTICS_REPORT_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(60);

    private final CsvReporter myReporter;

    public CsvMeterRegistry(final CsvConfig config, final Clock clock, final File outputDirectory)
    {
        this(config, clock, HierarchicalNameMapper.DEFAULT, outputDirectory);
    }

    public CsvMeterRegistry(final CsvConfig config, final Clock clock, final HierarchicalNameMapper nameMapper,
            final File outputDirectory)
    {
        this(config, clock, nameMapper, new MetricRegistry(), outputDirectory);
    }

    public CsvMeterRegistry(final CsvConfig config, final Clock clock, final HierarchicalNameMapper nameMapper,
            final MetricRegistry metricRegistry, final File outputDirectory)
    {
        this(config, clock, nameMapper, metricRegistry, defaultCsvReporter(metricRegistry, outputDirectory));
    }

    public CsvMeterRegistry(final CsvConfig config, final Clock clock, final HierarchicalNameMapper nameMapper,
            final MetricRegistry metricRegistry, final CsvReporter csvReporter)
    {
        super(config, metricRegistry, nameMapper, clock);
        myReporter = csvReporter;
    }

    private static CsvReporter defaultCsvReporter(final MetricRegistry metricRegistry, final File outputDirectory)
    {
        if (!outputDirectory.exists() && !outputDirectory.mkdirs())
        {
            LOG.warn("Failed to create statistics directory: {}, csv files will not be generated",
                    outputDirectory);
        }
        return CsvReporter.forRegistry(metricRegistry).convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.SECONDS)
                .build(outputDirectory);
    }

    public void stop()
    {
        myReporter.stop();
    }

    public void start()
    {
        myReporter.start(DEFAULT_STATISTICS_REPORT_INTERVAL_IN_MS, DEFAULT_STATISTICS_REPORT_INTERVAL_IN_MS,
                TimeUnit.MILLISECONDS);
    }

    // Only for tests
    void report()
    {
        myReporter.report();
    }

    @Override
    public void close()
    {
        stop();
        super.close();
    }

    @Override
    protected Double nullGaugeValue()
    {
        return Double.NaN;
    }
}
