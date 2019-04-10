/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import java.net.InetSocketAddress;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestHTTPServerProperties
{
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 8080;

    @Test
    public void testDefaultValues()
    {
        InetSocketAddress expectedAddress = new InetSocketAddress(DEFAULT_HOST, DEFAULT_PORT);

        Properties properties = new Properties();

        HTTPServerProperties httpServerProperties = HTTPServerProperties.from(properties);

        assertThat(httpServerProperties.getAddress()).isEqualTo(expectedAddress);
    }

    @Test
    public void testSetAddress()
    {
        String host = "127.0.0.2";
        int port = 8081;
        InetSocketAddress expectedAddress = new InetSocketAddress(host, port);

        Properties properties = new Properties();
        properties.put("http.server.host", host);
        properties.put("http.server.port", Integer.toString(port));

        HTTPServerProperties httpServerProperties = HTTPServerProperties.from(properties);

        assertThat(httpServerProperties.getAddress()).isEqualTo(expectedAddress);
    }

    @Test
    public void testSetInvalidPort()
    {
        String host = "127.0.0.2";

        Properties properties = new Properties();
        properties.put("http.server.host", host);
        properties.put("http.server.port", "abc");

        assertThatExceptionOfType(NumberFormatException.class).isThrownBy(() -> HTTPServerProperties.from(properties));
    }
}
