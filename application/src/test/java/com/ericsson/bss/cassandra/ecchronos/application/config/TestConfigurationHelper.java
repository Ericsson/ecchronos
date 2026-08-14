/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

public class TestConfigurationHelper
{
    private static final String CONFIG_PROPERTY = "test.ecchronos.config";

    @After
    public void tearDown()
    {
        System.clearProperty(CONFIG_PROPERTY);
    }

    @Test
    public void testLoadConfigurationFromConfiguredPath() throws IOException, ConfigurationException
    {
        Path directory = Files.createTempDirectory("ecchronos-config");
        Path configuration = directory.resolve("test.yml");

        Files.writeString(configuration, "value: expected\n");
        System.setProperty(CONFIG_PROPERTY, directory.toString());

        ConfigurationHelper helper = new ConfigurationHelper(CONFIG_PROPERTY);

        TestConfiguration result = helper.getConfiguration("test.yml", TestConfiguration.class);

        assertThat(result.value).isEqualTo("expected");
    }

    @Test
    public void testMalformedConfigurationThrowsConfigurationException() throws IOException
    {
        Path directory = Files.createTempDirectory("ecchronos-config");
        Path configuration = directory.resolve("invalid.yml");

        Files.writeString(configuration, "value: [invalid\n");
        System.setProperty(CONFIG_PROPERTY, directory.toString());

        ConfigurationHelper helper = new ConfigurationHelper(CONFIG_PROPERTY);

        assertThrows(
                ConfigurationException.class,
                () -> helper.getConfiguration("invalid.yml", TestConfiguration.class));
    }

    @Test
    public void testEmptyConfigurationThrowsConfigurationException() throws IOException
    {
        Path directory = Files.createTempDirectory("ecchronos-config");
        Path configuration = directory.resolve("empty.yml");

        Files.writeString(configuration, "");
        System.setProperty(CONFIG_PROPERTY, directory.toString());

        ConfigurationHelper helper = new ConfigurationHelper(CONFIG_PROPERTY);

        assertThrows(
                ConfigurationException.class,
                () -> helper.getConfiguration("empty.yml", TestConfiguration.class));
    }

    public static class TestConfiguration
    {
        public String value;
    }
}
