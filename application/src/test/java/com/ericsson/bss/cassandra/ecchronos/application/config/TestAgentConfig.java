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

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.AgentConnectionConfig;
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
    public void testAgentDefaultConfigFalse()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().isEnabled()).isFalse();
    }

    @Test
    public void testDefaultAgentType()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getType()).isEqualTo(AgentConnectionConfig.ConnectionType.datacenterAware);
    }

    @Test
    public void testDefaultContactPoints()
    {
        assertThat(nativeConnection.getAgentConnectionConfig()).isNotNull();
        assertThat(nativeConnection.getAgentConnectionConfig().getContactPoints().get("127.0.0.1").getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getAgentConnectionConfig().getContactPoints().get("127.0.0.2").getPort()).isEqualTo(9042);
        assertThat(nativeConnection.getAgentConnectionConfig().getContactPoints().size()).isEqualTo(2);
    }

    @Test
    public void testDefaultDatacenterAware()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getDatacenterAware()).isNotNull();
        assertThat(nativeConnection
            .getAgentConnectionConfig()
            .getDatacenterAware()
            .getDatacenterAware()
            .get("datacenter1").getName()).isEqualTo("datacenter1");
    }

    @Test
    public void testDefaultRackAware()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getRackAware()).isNotNull();
        assertThat(nativeConnection
            .getAgentConnectionConfig()
            .getRackAware()
            .getRackAware().get("rack1")
            .getDatacenterName()
            ).isEqualTo("datacenter1");
    }

    @Test
    public void testDefaultHostAware()
    {
        assertThat(nativeConnection.getAgentConnectionConfig().getHostAware()).isNotNull();
        assertThat(nativeConnection
            .getAgentConnectionConfig()
            .getHostAware().getHosts()
            .get("127.0.0.1").getPort())
        .isEqualTo(9042);

        assertThat(nativeConnection
            .getAgentConnectionConfig()
            .getHostAware().getHosts()
            .get("127.0.0.2").getPort())
        .isEqualTo(9042);

        assertThat(nativeConnection
            .getAgentConnectionConfig()
            .getHostAware().getHosts()
            .get("127.0.0.3").getPort())
        .isEqualTo(9042);

        assertThat(nativeConnection
            .getAgentConnectionConfig()
            .getHostAware().getHosts()
            .get("127.0.0.4").getPort())
        .isEqualTo(9042);
    }
}