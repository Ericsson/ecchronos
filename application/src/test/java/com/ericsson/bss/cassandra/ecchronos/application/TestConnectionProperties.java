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

public class TestConnectionProperties
{
    private static final String DEFAULT_NATIVE_HOST = "localhost";
    private static final String DEFAULT_JMX_HOST = "localhost";
    private static final int DEFAULT_NATIVE_PORT = 9042;
    private static final int DEFAULT_JMX_PORT = 7199;

    @Test
    public void testDefaultValues()
    {
        Properties properties = new Properties();

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeHost()).isEqualTo(DEFAULT_NATIVE_HOST);
        assertThat(connectionProperties.getJmxHost()).isEqualTo(DEFAULT_JMX_HOST);
        assertThat(connectionProperties.getNativePort()).isEqualTo(DEFAULT_NATIVE_PORT);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(DEFAULT_JMX_PORT);
    }

    @Test
    public void testSetNativeHost()
    {
        String expectedNativeHost = "127.0.0.1";

        Properties properties = new Properties();
        properties.put("connection.native.host", expectedNativeHost);

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeHost()).isEqualTo(expectedNativeHost);
        assertThat(connectionProperties.getJmxHost()).isEqualTo(DEFAULT_JMX_HOST);
        assertThat(connectionProperties.getNativePort()).isEqualTo(DEFAULT_NATIVE_PORT);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(DEFAULT_JMX_PORT);
    }

    @Test
    public void testSetJmxHost()
    {
        String expectedJmxHost = "127.0.0.1";

        Properties properties = new Properties();
        properties.put("connection.jmx.host", expectedJmxHost);

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeHost()).isEqualTo(DEFAULT_NATIVE_HOST);
        assertThat(connectionProperties.getJmxHost()).isEqualTo(expectedJmxHost);
        assertThat(connectionProperties.getNativePort()).isEqualTo(DEFAULT_NATIVE_PORT);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(DEFAULT_JMX_PORT);
    }

    @Test
    public void testSetNativePort()
    {
        int expectedNativePort = 9999;

        Properties properties = new Properties();
        properties.put("connection.native.port", Integer.toString(expectedNativePort));

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeHost()).isEqualTo(DEFAULT_NATIVE_HOST);
        assertThat(connectionProperties.getJmxHost()).isEqualTo(DEFAULT_JMX_HOST);
        assertThat(connectionProperties.getNativePort()).isEqualTo(expectedNativePort);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(DEFAULT_JMX_PORT);
    }

    @Test
    public void testSetJmxPort()
    {
        int expectedJmxPort = 7100;

        Properties properties = new Properties();
        properties.put("connection.jmx.port", Integer.toString(expectedJmxPort));

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeHost()).isEqualTo(DEFAULT_NATIVE_HOST);
        assertThat(connectionProperties.getJmxHost()).isEqualTo(DEFAULT_JMX_HOST);
        assertThat(connectionProperties.getNativePort()).isEqualTo(DEFAULT_NATIVE_PORT);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(expectedJmxPort);
    }

    @Test
    public void testSetAll()
    {
        String expectedNativeHost = "127.0.0.1";
        String expectedJmxHost = "127.0.0.1";
        int expectedNativePort = 9999;
        int expectedJmxPort = 7100;

        Properties properties = new Properties();
        properties.put("connection.native.host", expectedNativeHost);
        properties.put("connection.jmx.host", expectedJmxHost);
        properties.put("connection.native.port", Integer.toString(expectedNativePort));
        properties.put("connection.jmx.port", Integer.toString(expectedJmxPort));

        ConnectionProperties connectionProperties = ConnectionProperties.from(properties);

        assertThat(connectionProperties.getNativeHost()).isEqualTo(expectedNativeHost);
        assertThat(connectionProperties.getJmxHost()).isEqualTo(expectedJmxHost);
        assertThat(connectionProperties.getNativePort()).isEqualTo(expectedNativePort);
        assertThat(connectionProperties.getJmxPort()).isEqualTo(expectedJmxPort);
    }
}
