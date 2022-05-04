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
        File file = new File(classLoader.getResource("security.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Security config = objectMapper.readValue(file, Security.class);

        Credentials expectedCredentials = new Credentials(false, "cassandra", "cassandra");

        TLSConfig cqlTlsConfig = new TLSConfig();
        cqlTlsConfig.setEnabled(false);
        cqlTlsConfig.setKeystore("/path/to/keystore");
        cqlTlsConfig.setKeystore_password("ecchronos");
        cqlTlsConfig.setTruststore("/path/to/truststore");
        cqlTlsConfig.setTruststore_password("ecchronos");
        cqlTlsConfig.setCertificate(null);
        cqlTlsConfig.setCertificate_key(null);
        cqlTlsConfig.setCertificate_authorities(null);
        cqlTlsConfig.setProtocol("TLSv1.2");
        cqlTlsConfig.setAlgorithm(null);
        cqlTlsConfig.setStore_type("JKS");
        cqlTlsConfig.setCipher_suites(null);
        cqlTlsConfig.setRequire_endpoint_verification(false);

        TLSConfig jmxTlsConfig = new TLSConfig();
        jmxTlsConfig.setEnabled(false);
        jmxTlsConfig.setKeystore("/path/to/keystore");
        jmxTlsConfig.setKeystore_password("ecchronos");
        jmxTlsConfig.setTruststore("/path/to/truststore");
        jmxTlsConfig.setTruststore_password("ecchronos");
        jmxTlsConfig.setProtocol("TLSv1.2");
        jmxTlsConfig.setCipher_suites(null);

        assertThat(config.getCql().getCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getJmx().getCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getCql().getTls()).isEqualTo(cqlTlsConfig);
        assertThat(config.getJmx().getTls()).isEqualTo(jmxTlsConfig);
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

        TLSConfig cqlTlsConfig = new TLSConfig();
        cqlTlsConfig.setEnabled(true);
        cqlTlsConfig.setKeystore("/path/to/cql/keystore");
        cqlTlsConfig.setKeystore_password("cqlkeystorepassword");
        cqlTlsConfig.setTruststore("/path/to/cql/truststore");
        cqlTlsConfig.setTruststore_password("cqltruststorepassword");
        cqlTlsConfig.setProtocol("TLSv1.2");
        cqlTlsConfig.setAlgorithm("SunX509");
        cqlTlsConfig.setStore_type("JKS");
        cqlTlsConfig.setCipher_suites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");
        cqlTlsConfig.setRequire_endpoint_verification(true);

        TLSConfig jmxTlsConfig = new TLSConfig();
        jmxTlsConfig.setEnabled(true);
        jmxTlsConfig.setKeystore("/path/to/jmx/keystore");
        jmxTlsConfig.setKeystore_password("jmxkeystorepassword");
        jmxTlsConfig.setTruststore("/path/to/jmx/truststore");
        jmxTlsConfig.setTruststore_password("jmxtruststorepassword");
        jmxTlsConfig.setProtocol("TLSv1.2");
        jmxTlsConfig.setCipher_suites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");

        assertThat(config.getCql().getCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getJmx().getCredentials()).isEqualTo(expectedJmxCredentials);
        assertThat(config.getCql().getTls()).isEqualTo(cqlTlsConfig);
        assertThat(config.getJmx().getTls()).isEqualTo(jmxTlsConfig);
    }

    @Test
    public void testEnabledWithCertificate() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("enabled_certificate_security.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        Security config = objectMapper.readValue(file, Security.class);

        Credentials expectedCqlCredentials = new Credentials(true, "cqluser", "cqlpassword");

        TLSConfig cqlTlsConfig = new TLSConfig();
        cqlTlsConfig.setEnabled(true);
        cqlTlsConfig.setCertificate("/path/to/cql/certificate");
        cqlTlsConfig.setCertificate_key("/path/to/cql/certificate_key");
        cqlTlsConfig.setCertificate_authorities("/path/to/cql/certificate_authorities");
        cqlTlsConfig.setProtocol("TLSv1.2");
        cqlTlsConfig.setCipher_suites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");
        cqlTlsConfig.setRequire_endpoint_verification(true);

        assertThat(config.getCql().getCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getCql().getTls()).isEqualTo(cqlTlsConfig);
    }
}
