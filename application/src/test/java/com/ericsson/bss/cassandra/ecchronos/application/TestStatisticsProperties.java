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
package com.ericsson.bss.cassandra.ecchronos.application;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStatisticsProperties
{
    private static final boolean DEFAULT_STATISTICS_ENABLED = true;
    private static final String DEFAULT_STATISTICS_DIRECTORY = "./statistics";

    @Rule
    public TemporaryFolder myTemporaryFolder = new TemporaryFolder();

    private File myDefaultStatisticsDirectory;

    @Before
    public void createDefaultDirectory()
    {
        myDefaultStatisticsDirectory = new File(DEFAULT_STATISTICS_DIRECTORY);
        myDefaultStatisticsDirectory.mkdir();
    }

    @After
    public void removeDefaultDirectory()
    {
        myDefaultStatisticsDirectory.delete();
    }

    @Test
    public void testDefaultValues() throws ConfigurationException
    {
        Properties properties = new Properties();

        StatisticsProperties statisticsProperties = StatisticsProperties.from(properties);

        assertThat(statisticsProperties.isEnabled()).isEqualTo(DEFAULT_STATISTICS_ENABLED);
        assertThat(statisticsProperties.getStatisticsDirectory()).isEqualTo(myDefaultStatisticsDirectory);
    }

    @Test
    public void testDisable() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("statistics.enabled", "false");

        StatisticsProperties statisticsProperties = StatisticsProperties.from(properties);

        assertThat(statisticsProperties.isEnabled()).isEqualTo(false);
        assertThat(statisticsProperties.getStatisticsDirectory()).isEqualTo(myDefaultStatisticsDirectory);
    }

    @Test
    public void testDifferentStatisticsDirectory() throws IOException, ConfigurationException
    {
        File expectedStatisticsDirectory = myTemporaryFolder.newFolder();
        expectedStatisticsDirectory.deleteOnExit();

        Properties properties = new Properties();
        properties.put("statistics.directory", expectedStatisticsDirectory.toString());

        StatisticsProperties statisticsProperties = StatisticsProperties.from(properties);

        assertThat(statisticsProperties.isEnabled()).isEqualTo(DEFAULT_STATISTICS_ENABLED);
        assertThat(statisticsProperties.getStatisticsDirectory()).isEqualTo(expectedStatisticsDirectory);
    }

    @Test
    public void testSetAllOptions() throws IOException, ConfigurationException
    {
        File expectedStatisticsDirectory = myTemporaryFolder.newFolder();
        expectedStatisticsDirectory.deleteOnExit();

        Properties properties = new Properties();
        properties.put("statistics.enabled", "false");
        properties.put("statistics.directory", expectedStatisticsDirectory.toString());

        StatisticsProperties statisticsProperties = StatisticsProperties.from(properties);

        assertThat(statisticsProperties.isEnabled()).isEqualTo(false);
        assertThat(statisticsProperties.getStatisticsDirectory()).isEqualTo(expectedStatisticsDirectory);
    }

    @Test (expected = ConfigurationException.class)
    public void testEnabledAndInvalidStatisticsPath() throws IOException, ConfigurationException
    {
        File expectedNonExistingStatisticsDirectory = myTemporaryFolder.newFolder();
        expectedNonExistingStatisticsDirectory.delete();

        Properties properties = new Properties();
        properties.put("statistics.enabled", "true");
        properties.put("statistics.directory", expectedNonExistingStatisticsDirectory.toURI().toString());

        StatisticsProperties.from(properties);
    }

    @Test
    public void testDisabledAndInvalidStatisticsPath() throws IOException, ConfigurationException
    {
        File expectedNonExistingStatisticsDirectory = myTemporaryFolder.newFolder();
        expectedNonExistingStatisticsDirectory.delete();

        Properties properties = new Properties();
        properties.put("statistics.enabled", "false");
        properties.put("statistics.directory", expectedNonExistingStatisticsDirectory.toString());

        StatisticsProperties statisticsProperties = StatisticsProperties.from(properties);

        assertThat(statisticsProperties.isEnabled()).isEqualTo(false);
        assertThat(statisticsProperties.getStatisticsDirectory()).isEqualTo(expectedNonExistingStatisticsDirectory);
    }
}
