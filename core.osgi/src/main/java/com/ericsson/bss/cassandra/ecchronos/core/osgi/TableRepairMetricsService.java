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
package com.ericsson.bss.cassandra.ecchronos.core.osgi;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetricsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@Component(service = {TableRepairMetrics.class, TableRepairMetricsProvider.class})
@Designate(ocd = TableRepairMetricsService.Configuration.class)
public final class TableRepairMetricsService implements TableRepairMetrics, TableRepairMetricsProvider
{
    private static final long DEFAULT_STATISTICS_REPORT_INTERVAL_IN_SECONDS = 60L;

    @Reference(service = TableStorageStates.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile TableStorageStates myTableStorageStates;

    private volatile TableRepairMetricsImpl myDelegateTableRepairMetrics;

    @Activate
    public void activate(Configuration configuration)
    {
        String statisticsDirectory = configuration.metricsDirectory();
        long reportIntervalInSeconds = configuration.metricsReportIntervalInSeconds();

        myDelegateTableRepairMetrics = TableRepairMetricsImpl.builder()
                .withTableStorageStates(myTableStorageStates)
                .withStatisticsDirectory(statisticsDirectory)
                .withMetricRegistry(new MetricRegistry())
                .withReportInterval(reportIntervalInSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Deactivate
    public void deactivate()
    {
        myDelegateTableRepairMetrics.close();
    }

    @Override
    public void repairState(TableReference tableReference, int repairedRanges, int notRepairedRanges)
    {
        myDelegateTableRepairMetrics.repairState(tableReference, repairedRanges, notRepairedRanges);
    }

    @Override
    public Optional<Double> getRepairRatio(TableReference tableReference)
    {
        return myDelegateTableRepairMetrics.getRepairRatio(tableReference);
    }

    @Override
    public void lastRepairedAt(TableReference tableReference, long lastRepairedAt)
    {
        myDelegateTableRepairMetrics.lastRepairedAt(tableReference, lastRepairedAt);
    }

    @Override
    public void remainingRepairTime(TableReference tableReference, long remainingRepairTime)
    {
        myDelegateTableRepairMetrics.remainingRepairTime(tableReference, remainingRepairTime);
    }

    @Override
    public void repairTiming(TableReference tableReference, long timeTaken, TimeUnit timeUnit, boolean successful)
    {
        myDelegateTableRepairMetrics.repairTiming(tableReference, timeTaken, timeUnit, successful);
    }

    @Override
    public void repairFailedAttempts(TableReference tableReference)
    {
        myDelegateTableRepairMetrics.repairFailedAttempts(tableReference);
    }

    @Override
    public void resetRepairFailedAttempts(TableReference tableReference)
    {
        myDelegateTableRepairMetrics.resetRepairFailedAttempts(tableReference);
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Metrics directory", description = "The directory which the repair metrics will be stored in")
        String metricsDirectory();

        @AttributeDefinition(name = "Report interval in seconds", description = "The interval in which the metrics will be reported")
        long metricsReportIntervalInSeconds() default DEFAULT_STATISTICS_REPORT_INTERVAL_IN_SECONDS;
    }
}
