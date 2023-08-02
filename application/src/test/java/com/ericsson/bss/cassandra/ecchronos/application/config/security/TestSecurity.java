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
package com.ericsson.bss.cassandra.ecchronos.application.config.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestSecurity
{
    @Test
    public void testDefault() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("security.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Security config = objectMapper.readValue(file, Security.class);

        Credentials expectedCredentials = new Credentials(false, "cassandra", "cassandra");

        CqlTLSConfig cqlTLSConfig = new CqlTLSConfig(false, "/path/to/keystore", "ecchronos",
                "/path/to/truststore", "ecchronos");
        cqlTLSConfig.setStoreType("JKS");
        cqlTLSConfig.setProtocol("TLSv1.2");

        JmxTLSConfig jmxTLSConfig = new JmxTLSConfig(false, "/path/to/keystore",
                "ecchronos", "/path/to/truststore", "ecchronos");
        jmxTLSConfig.setProtocol("TLSv1.2");
        jmxTLSConfig.setCipherSuites(null);

        assertThat(config.getCqlSecurity().getCqlCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getJmxSecurity().getJmxCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getCqlSecurity().getCqlTlsConfig()).isEqualTo(cqlTLSConfig);
        assertThat(config.getJmxSecurity().getJmxTlsConfig()).isEqualTo(jmxTLSConfig);
    }

    @Test
    public void testCqlAndJmxEnabledKeyStore() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("security/enabled_keystore_jmxandcql.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Security config = objectMapper.readValue(file, Security.class);

        Credentials expectedCqlCredentials = new Credentials(true, "cqluser", "cqlpassword");
        Credentials expectedJmxCredentials = new Credentials(true, "jmxuser", "jmxpassword");

        CqlTLSConfig cqlTLSConfig = new CqlTLSConfig(true, "/path/to/cql/keystore",
                "cqlkeystorepassword", "/path/to/cql/truststore","cqltruststorepassword");
        cqlTLSConfig.setStoreType("JKS");
        cqlTLSConfig.setAlgorithm("SunX509");
        cqlTLSConfig.setProtocol("TLSv1.2");
        cqlTLSConfig.setCipherSuites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");
        cqlTLSConfig.setRequireEndpointVerification(true);

        JmxTLSConfig jmxTLSConfig = new JmxTLSConfig(true, "/path/to/jmx/keystore",
                "jmxkeystorepassword", "/path/to/jmx/truststore", "jmxtruststorepassword");
        jmxTLSConfig.setProtocol("TLSv1.2");
        jmxTLSConfig.setCipherSuites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");

        assertThat(config.getCqlSecurity().getCqlCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getJmxSecurity().getJmxCredentials()).isEqualTo(expectedJmxCredentials);
        assertThat(config.getCqlSecurity().getCqlTlsConfig()).isEqualTo(cqlTLSConfig);
        assertThat(config.getJmxSecurity().getJmxTlsConfig()).isEqualTo(jmxTLSConfig);
    }

    @Test
    public void testCqlEnabledWithCertificate() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("security/enabled_pem_cql.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Security config = objectMapper.readValue(file, Security.class);

        Credentials expectedCqlCredentials = new Credentials(true, "cqluser", "cqlpassword");

        CqlTLSConfig cqlTLSConfig = new CqlTLSConfig(true, "/path/to/cql/certificate",
                "/path/to/cql/certificate_key", "/path/to/cql/certificate_authorities");
        cqlTLSConfig.setProtocol("TLSv1.2");
        cqlTLSConfig.setCipherSuites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");
        cqlTLSConfig.setRequireEndpointVerification(true);

        assertThat(config.getCqlSecurity().getCqlCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getCqlSecurity().getCqlTlsConfig()).isEqualTo(cqlTLSConfig);
    }

    @Test
    public void testJmxEnabledWithCertificate()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("security/enabled_pem_jmx.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        assertThatExceptionOfType(IOException.class).isThrownBy(() -> objectMapper.readValue(file, Security.class));
    }

    @Test
    public void testCqlEnabledWithNothing()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("security/enabled_nokeystore_nopem_cql.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        assertThatExceptionOfType(ValueInstantiationException.class).isThrownBy(() -> objectMapper.readValue(file, Security.class));
    }

    @Test
    public void testJmxEnabledWithNothing()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("security/enabled_nokeystore_nopem_jmx.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        assertThatExceptionOfType(ValueInstantiationException.class).isThrownBy(() -> objectMapper.readValue(file, Security.class));
    }
}
