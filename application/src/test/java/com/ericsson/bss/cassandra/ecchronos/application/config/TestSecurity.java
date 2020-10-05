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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSecurity
{
    @Test
    public void testDefault() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("default_security.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Security config = objectMapper.readValue(file, Security.class);

        Credentials expectedCredentials = new Credentials(false, "cassandra", "cassandra");

        assertThat(config.getCql().getCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getJmx().getCredentials()).isEqualTo(expectedCredentials);
    }

    @Test
    public void testEnabled() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("enabled_security.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Security config = objectMapper.readValue(file, Security.class);

        Credentials expectedCqlCredentials = new Credentials(true, "cqluser", "cqlpassword");
        Credentials expectedJmxCredentials = new Credentials(true, "jmxuser", "jmxpassword");

        assertThat(config.getCql().getCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getJmx().getCredentials()).isEqualTo(expectedJmxCredentials);
    }
}
