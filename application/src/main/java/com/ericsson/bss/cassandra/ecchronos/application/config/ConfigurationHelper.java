/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import com.ericsson.bss.cassandra.ecchronos.application.ConfigurationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ConfigurationHelper
{
    private static final String CONFIGURATION_DIRECTORY_PATH = "ecchronos.config";

    public static final ConfigurationHelper DEFAULT_INSTANCE = new ConfigurationHelper(CONFIGURATION_DIRECTORY_PATH);

    private final String configurationDirectory;
    private final boolean usePath;

    public ConfigurationHelper(final String theConfigurationDirectory)
    {
        this.configurationDirectory = theConfigurationDirectory;
        this.usePath = System.getProperty(this.configurationDirectory) != null;
    }

    public final boolean usePath()
    {
        return usePath;
    }

    public final <T> T getConfiguration(final String file, final Class<T> clazz) throws ConfigurationException
    {
        if (usePath())
        {
            return getConfiguration(configFile(file), clazz);
        }
        else
        {
            return getFileFromClassPath(file, clazz);
        }
    }

    public final File configFile(final String configFile)
    {
        return new File(getConfigPath().toFile(), configFile);
    }

    public final Path getConfigPath()
    {
        return FileSystems.getDefault().getPath(System.getProperty(configurationDirectory));
    }

    private <T> T getFileFromClassPath(final String file, final Class<T> clazz) throws ConfigurationException
    {
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        return getConfiguration(loader.getResourceAsStream(file), clazz);
    }

    private <T> T getConfiguration(final File configurationFile, final Class<T> clazz) throws ConfigurationException
    {
        try
        {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

            T config = objectMapper.readValue(configurationFile, clazz);
            if (config == null)
            {
                throw new IOException("parsed config is null");
            }
            return config;
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to load configuration file " + configurationFile, e);
        }
    }

    private <T> T getConfiguration(final InputStream configurationFile,
                                   final Class<T> clazz) throws ConfigurationException
    {
        try
        {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

            T config = objectMapper.readValue(configurationFile, clazz);
            if (config == null)
            {
                throw new IOException("parsed config is null");
            }
            return config;
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to load configuration file from classpath", e);
        }
    }
}
