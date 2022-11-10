/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import io.micrometer.core.instrument.Clock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCsvMeterRegistry
{
    @Rule
    public TemporaryFolder metricsFolder = new TemporaryFolder();

    @Test
    public void testCsvMeterRegistry()
    {
        CsvMeterRegistry csvMeterRegistry = new CsvMeterRegistry(CsvConfig.DEFAULT, Clock.SYSTEM,
                metricsFolder.getRoot().getAbsoluteFile());
        assertThat(csvMeterRegistry).isNotNull();
        csvMeterRegistry.close();
        assertThat(csvMeterRegistry.isClosed()).isTrue();
    }

    @Test
    public void testCsvMeterRegistryGauge() throws IOException
    {
        CsvMeterRegistry csvMeterRegistry = new CsvMeterRegistry(CsvConfig.DEFAULT, Clock.SYSTEM,
                metricsFolder.getRoot().getAbsoluteFile());
        assertThat(csvMeterRegistry).isNotNull();
        csvMeterRegistry.start();
        csvMeterRegistry.gauge("test", 1.0);
        csvMeterRegistry.report();
        assertThat(getCsvMetricValue("test", 1)).isEqualTo(1.0);
    }

    @Test
    public void testCsvMeterRegistryMetricReportedToFile() throws IOException
    {
        CsvMeterRegistry csvMeterRegistry = new CsvMeterRegistry(CsvConfig.DEFAULT, Clock.SYSTEM,
                metricsFolder.getRoot().getAbsoluteFile());
        assertThat(csvMeterRegistry).isNotNull();
        csvMeterRegistry.start();
        csvMeterRegistry.gauge("test", 1.0);
        csvMeterRegistry.report();
        assertThat(getCsvMetricValue("test", 1)).isEqualTo(1.0);
    }

    private double getCsvMetricValue(String metric, int csvPos) throws IOException
    {
        String metricFile = metric + ".csv";
        try(BufferedReader bufferedReader = new BufferedReader(
                new FileReader(new File(metricsFolder.getRoot(), metricFile))))
        {
            bufferedReader.readLine(); // CSV header
            String line = bufferedReader.readLine();

            String[] splits = line.split(",");

            return Double.parseDouble(splits[csvPos]);
        }
    }
}
