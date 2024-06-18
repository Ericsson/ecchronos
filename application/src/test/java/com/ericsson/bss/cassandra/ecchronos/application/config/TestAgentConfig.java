/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.NativeConnection;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAgentConfig
{
    private static final String DEFAULT_AGENT_FILE_NAME = "agent_set.yml";
    private static Config config;
    private static NativeConnection nativeConnection;
    
    @Before
    public void setup() throws StreamReadException, DatabindException, IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        File file = new File(classLoader.getResource(DEFAULT_AGENT_FILE_NAME).getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        config = objectMapper.readValue(file, Config.class);

        ConnectionConfig connection = config.getConnectionConfig();

        nativeConnection = connection.getCqlConnection();
    }

    @Test
    public void testDatacenterProperties() throws Exception
    {
        assertThat(nativeConnection.getDatacenterAwareConfig().getDatacenterConfig().size()).isEqualTo(2);

        assertThat(nativeConnection
            .getDatacenterAwareConfig()
            .getDatacenterConfig().get("datacenter1").getHosts().get(0).getHost()).isEqualTo("127.0.0.1");
        assertThat(nativeConnection
                .getDatacenterAwareConfig()
                .getDatacenterConfig().get("datacenter1").getHosts().get(0).getPort()).isEqualTo(9042);
        assertThat(nativeConnection
                        .getDatacenterAwareConfig()
                        .getDatacenterConfig().get("datacenter1").getHosts().size()).isEqualTo(2);

        assertThat(nativeConnection
                .getDatacenterAwareConfig()
                .getDatacenterConfig().get("datacenter2").getHosts().get(0).getHost()).isEqualTo("127.0.0.1");
        assertThat(nativeConnection
                .getDatacenterAwareConfig()
                .getDatacenterConfig().get("datacenter2").getHosts().get(0).getPort()).isEqualTo(9042);
        assertThat(nativeConnection
                .getDatacenterAwareConfig()
                .getDatacenterConfig().get("datacenter2").getHosts().size()).isEqualTo(2);
    }
}