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

        TLSConfig tlsConfig = new TLSConfig();
        tlsConfig.setEnabled(false);
        tlsConfig.setKeystore("<keystore path>");
        tlsConfig.setKeystore_password("ecchronos");
        tlsConfig.setTruststore("<truststore path>");
        tlsConfig.setTruststore_password("ecchronos");
        tlsConfig.setProtocol("TLSv1.2");
        tlsConfig.setAlgorithm(null);
        tlsConfig.setStore_type("JKS");
        tlsConfig.setCipher_suites(null);
        tlsConfig.setRequire_endpoint_verification(false);

        assertThat(config.getCql().getCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getJmx().getCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getCql().getTls()).isEqualTo(tlsConfig);
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

        TLSConfig tlsConfig = new TLSConfig();
        tlsConfig.setEnabled(true);
        tlsConfig.setKeystore("path_to_keystore");
        tlsConfig.setKeystore_password("keystorepassword");
        tlsConfig.setTruststore("path_to_truststore");
        tlsConfig.setTruststore_password("truststorepassword");
        tlsConfig.setProtocol("TLSv1.2");
        tlsConfig.setAlgorithm("SunX509");
        tlsConfig.setStore_type("JKS");
        tlsConfig.setCipher_suites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");
        tlsConfig.setRequire_endpoint_verification(true);

        assertThat(config.getCql().getCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getJmx().getCredentials()).isEqualTo(expectedJmxCredentials);
        assertThat(config.getCql().getTls()).isEqualTo(tlsConfig);
    }
}
