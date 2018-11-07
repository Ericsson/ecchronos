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

import java.io.File;
import java.util.Properties;

public class StatisticsProperties
{
    private static final String CONFIG_STATISTICS_ENABLED = "statistics.enabled";
    private static final String CONFIG_STATISTICS_DIRECTORY = "statistics.directory";

    private static final String DEFAULT_STATISTICS_ENABLED = "true";
    private static final String DEFAULT_STATISTICS_DIRECTORY = "./statistics";

    private final boolean isEnabled;
    private final File myStatisticsDirectory;

    private StatisticsProperties(boolean enabled, File statisticsDirectory)
    {
        isEnabled = enabled;
        myStatisticsDirectory = statisticsDirectory;
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    public File getStatisticsDirectory()
    {
        return myStatisticsDirectory;
    }

    public static StatisticsProperties from(Properties properties) throws ConfigurationException
    {
        boolean enabled = Boolean.parseBoolean(properties.getProperty(CONFIG_STATISTICS_ENABLED, DEFAULT_STATISTICS_ENABLED));
        File statisticsDirectory = new File(properties.getProperty(CONFIG_STATISTICS_DIRECTORY, DEFAULT_STATISTICS_DIRECTORY));

        if (enabled && !statisticsDirectory.exists())
        {
            throw new ConfigurationException("Statistics directory '" + statisticsDirectory + "' does not exist");
        }

        return new StatisticsProperties(enabled, statisticsDirectory);
    }
}
