/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import org.junit.After;
import org.junit.Test;

import javax.management.remote.JMXConnector;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestJmxEnvironmentFactory
{
    @After
    public void clearSystemProperties()
    {
        System.clearProperty(DistributedJmxBuilder.JOLOKIA_CA_CERTIFICATE_PROPERTY);
        System.clearProperty(DistributedJmxBuilder.JOLOKIA_CLIENT_CERTIFICATE_PROPERTY);
        System.clearProperty(DistributedJmxBuilder.JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY);
        System.clearProperty(DistributedJmxBuilder.JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY);
        System.clearProperty(DistributedJmxBuilder.JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY);
    }

    @Test
    public void testNoCredentialsNoTls()
    {
        Map<String, Object> env = JmxEnvironmentFactory.create(null, null, false);
        assertThat(env).isEmpty();
    }

    @Test
    public void testCredentialsAdded()
    {
        String[] creds = {"user", "pass"};
        Map<String, Object> env = JmxEnvironmentFactory.create(() -> creds, null, false);
        assertThat(env).containsKey(JMXConnector.CREDENTIALS);
        assertThat((String[]) env.get(JMXConnector.CREDENTIALS)).containsExactly("user", "pass");
    }

    @Test
    public void testJolokiaSslSetsSystemProperties()
    {
        Map<String, String> tls = new HashMap<>();
        tls.put(DistributedJmxBuilder.ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY, "true");
        tls.put(DistributedJmxBuilder.JOLOKIA_CA_CERTIFICATE_PROPERTY, "/ca.crt");
        tls.put(DistributedJmxBuilder.JOLOKIA_CLIENT_CERTIFICATE_PROPERTY, "/client.crt");
        tls.put(DistributedJmxBuilder.JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY, "/client.key");
        tls.put(DistributedJmxBuilder.JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY, "RSA");
        tls.put(DistributedJmxBuilder.JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY, "true");

        JmxEnvironmentFactory.create(null, () -> tls, true);

        assertThat(System.getProperty(DistributedJmxBuilder.JOLOKIA_CA_CERTIFICATE_PROPERTY)).isEqualTo("/ca.crt");
        assertThat(System.getProperty(DistributedJmxBuilder.JOLOKIA_CLIENT_CERTIFICATE_PROPERTY)).isEqualTo("/client.crt");
        assertThat(System.getProperty(DistributedJmxBuilder.JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY)).isEqualTo("/client.key");
    }

    @Test
    public void testRmiTlsSetsSocketFactory()
    {
        Map<String, String> tls = new HashMap<>();
        tls.put("javax.net.ssl.keyStore", "/keystore.jks");
        tls.put("javax.net.ssl.keyStorePassword", "secret");

        Map<String, Object> env = JmxEnvironmentFactory.create(null, () -> tls, false);

        assertThat(env).containsKey("com.sun.jndi.rmi.factory.socket");
        assertThat(System.getProperty("javax.net.ssl.keyStore")).isEqualTo("/keystore.jks");

        // Cleanup
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStorePassword");
    }

    @Test
    public void testNullCredentialsSupplierReturnsNoCredentials()
    {
        Map<String, String> tls = new HashMap<>();
        Map<String, Object> env = JmxEnvironmentFactory.create(null, () -> tls, false);
        assertThat(env).doesNotContainKey(JMXConnector.CREDENTIALS);
    }
}
