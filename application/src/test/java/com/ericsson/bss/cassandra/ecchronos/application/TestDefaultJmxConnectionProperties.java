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

import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDefaultJmxConnectionProperties
{
    private static final String DEFAULT_JMX_HOST = "localhost";
    private static final int DEFAULT_JMX_PORT = 7199;

    @Test
    public void testDefaultValues()
    {
        Properties properties = new Properties();

        DefaultJmxConnectionProperties connectionProperties =DefaultJmxConnectionProperties.from(properties);

        assertThat(connectionProperties.getJmxHost()).isEqualTo(DEFAULT_JMX_HOST);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(DEFAULT_JMX_PORT);
    }

    @Test
    public void testSetJmxHost()
    {
        String expectedJmxHost = "127.0.0.1";

        Properties properties = new Properties();
        properties.put("connection.jmx.host", expectedJmxHost);

        DefaultJmxConnectionProperties connectionProperties =DefaultJmxConnectionProperties.from(properties);

        assertThat(connectionProperties.getJmxHost()).isEqualTo(expectedJmxHost);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(DEFAULT_JMX_PORT);
    }

    @Test
    public void testSetJmxPort()
    {
        int expectedJmxPort = 7100;

        Properties properties = new Properties();
        properties.put("connection.jmx.port", Integer.toString(expectedJmxPort));

        DefaultJmxConnectionProperties connectionProperties =DefaultJmxConnectionProperties.from(properties);

        assertThat(connectionProperties.getJmxHost()).isEqualTo(DEFAULT_JMX_HOST);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(expectedJmxPort);
    }

    @Test
    public void testSetAll()
    {
        String expectedJmxHost = "127.0.0.1";
        int expectedJmxPort = 7100;

        Properties properties = new Properties();
        properties.put("connection.jmx.host", expectedJmxHost);
        properties.put("connection.jmx.port", Integer.toString(expectedJmxPort));

        DefaultJmxConnectionProperties connectionProperties =DefaultJmxConnectionProperties.from(properties);

        assertThat(connectionProperties.getJmxHost()).isEqualTo(expectedJmxHost);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(expectedJmxPort);
    }
}
