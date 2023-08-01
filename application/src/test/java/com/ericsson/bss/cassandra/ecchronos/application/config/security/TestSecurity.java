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
        cqlTlsConfig.setKeyStorePath("/path/to/keystore");
        cqlTlsConfig.setKeyStorePassword("ecchronos");
        cqlTlsConfig.setTrustStorePath("/path/to/truststore");
        cqlTlsConfig.setTrustStorePassword("ecchronos");
        cqlTlsConfig.setCertificatePath(null);
        cqlTlsConfig.setCertificatePrivateKeyPath(null);
        cqlTlsConfig.setTrustCertificatePath(null);
        cqlTlsConfig.setProtocol("TLSv1.2");
        cqlTlsConfig.setAlgorithm(null);
        cqlTlsConfig.setStoreType("JKS");
        cqlTlsConfig.setCipherSuites(null);
        cqlTlsConfig.setRequireEndpointVerification(false);

        TLSConfig jmxTlsConfig = new TLSConfig();
        jmxTlsConfig.setEnabled(false);
        jmxTlsConfig.setKeyStorePath("/path/to/keystore");
        jmxTlsConfig.setKeyStorePassword("ecchronos");
        jmxTlsConfig.setTrustStorePath("/path/to/truststore");
        jmxTlsConfig.setTrustStorePassword("ecchronos");
        jmxTlsConfig.setProtocol("TLSv1.2");
        jmxTlsConfig.setCipherSuites(null);

        assertThat(config.getCqlSecurity().getCqlCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getJmxSecurity().getJmxCredentials()).isEqualTo(expectedCredentials);
        assertThat(config.getCqlSecurity().getCqlTlsConfig()).isEqualTo(cqlTlsConfig);
        assertThat(config.getJmxSecurity().getJmxTlsConfig()).isEqualTo(jmxTlsConfig);
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
        cqlTlsConfig.setKeyStorePath("/path/to/cql/keystore");
        cqlTlsConfig.setKeyStorePassword("cqlkeystorepassword");
        cqlTlsConfig.setTrustStorePath("/path/to/cql/truststore");
        cqlTlsConfig.setTrustStorePassword("cqltruststorepassword");
        cqlTlsConfig.setProtocol("TLSv1.2");
        cqlTlsConfig.setAlgorithm("SunX509");
        cqlTlsConfig.setStoreType("JKS");
        cqlTlsConfig.setCipherSuites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");
        cqlTlsConfig.setRequireEndpointVerification(true);

        TLSConfig jmxTlsConfig = new TLSConfig();
        jmxTlsConfig.setEnabled(true);
        jmxTlsConfig.setKeyStorePath("/path/to/jmx/keystore");
        jmxTlsConfig.setKeyStorePassword("jmxkeystorepassword");
        jmxTlsConfig.setTrustStorePath("/path/to/jmx/truststore");
        jmxTlsConfig.setTrustStorePassword("jmxtruststorepassword");
        jmxTlsConfig.setProtocol("TLSv1.2");
        jmxTlsConfig.setCipherSuites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");

        assertThat(config.getCqlSecurity().getCqlCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getJmxSecurity().getJmxCredentials()).isEqualTo(expectedJmxCredentials);
        assertThat(config.getCqlSecurity().getCqlTlsConfig()).isEqualTo(cqlTlsConfig);
        assertThat(config.getJmxSecurity().getJmxTlsConfig()).isEqualTo(jmxTlsConfig);
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
        cqlTlsConfig.setCertificatePath("/path/to/cql/certificate");
        cqlTlsConfig.setCertificatePrivateKeyPath("/path/to/cql/certificate_key");
        cqlTlsConfig.setTrustCertificatePath("/path/to/cql/certificate_authorities");
        cqlTlsConfig.setProtocol("TLSv1.2");
        cqlTlsConfig.setCipherSuites("VALID_CIPHER_SUITE,VALID_CIPHER_SUITE2");
        cqlTlsConfig.setRequireEndpointVerification(true);

        assertThat(config.getCqlSecurity().getCqlCredentials()).isEqualTo(expectedCqlCredentials);
        assertThat(config.getCqlSecurity().getCqlTlsConfig()).isEqualTo(cqlTlsConfig);
    }
}
