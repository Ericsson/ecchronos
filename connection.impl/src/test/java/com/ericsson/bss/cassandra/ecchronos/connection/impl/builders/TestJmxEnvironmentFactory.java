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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionStrategy;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.ConnectionUtils;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.JolokiaConnectionStrategy;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.RMIConnectionStrategy;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJmxEnvironmentFactory
{
    @After
    public void clearSystemProperties()
    {
        System.clearProperty(JolokiaConnectionStrategy.JOLOKIA_CA_CERTIFICATE_PROPERTY);
        System.clearProperty(JolokiaConnectionStrategy.JOLOKIA_CLIENT_CERTIFICATE_PROPERTY);
        System.clearProperty(JolokiaConnectionStrategy.JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY);
        System.clearProperty(JolokiaConnectionStrategy.JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY);
        System.clearProperty(JolokiaConnectionStrategy.JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY);
        System.clearProperty("javax.net.ssl.keyStore");
        System.clearProperty("javax.net.ssl.keyStorePassword");
        System.clearProperty("javax.net.ssl.trustStore");
        System.clearProperty("javax.net.ssl.trustStorePassword");
    }

    @Test
    public void testJolokiaBuilderDefaultPort()
    {
        ConnectionUtils connectionUtils = createConnectionUtils(null, HashMap::new);
        JolokiaConnectionStrategy strategy = JolokiaConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .build();
        assertThat(strategy).isNotNull();
    }

    @Test
    public void testJolokiaBuilderCustomPort()
    {
        ConnectionUtils connectionUtils = createConnectionUtils(null, HashMap::new);
        JolokiaConnectionStrategy strategy = JolokiaConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(9999)
                .build();
        assertThat(strategy).isNotNull();
    }

    @Test
    public void testRmiBuilderDefaultPort()
    {
        ConnectionUtils connectionUtils = createConnectionUtils(null, HashMap::new);
        RMIConnectionStrategy strategy = RMIConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .build();
        assertThat(strategy).isNotNull();
    }

    @Test
    public void testRmiBuilderCustomPort()
    {
        ConnectionUtils connectionUtils = createConnectionUtils(null, HashMap::new);
        RMIConnectionStrategy strategy = RMIConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(7200)
                .build();
        assertThat(strategy).isNotNull();
    }

    @Test
    public void testJolokiaConnectSetsJolokiaSslProperties()
    {
        Map<String, String> tls = new HashMap<>();
        tls.put(JolokiaConnectionStrategy.ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY, "true");
        tls.put(JolokiaConnectionStrategy.JOLOKIA_CA_CERTIFICATE_PROPERTY, "/ca.crt");
        tls.put(JolokiaConnectionStrategy.JOLOKIA_CLIENT_CERTIFICATE_PROPERTY, "/client.crt");
        tls.put(JolokiaConnectionStrategy.JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY, "/client.key");
        tls.put(JolokiaConnectionStrategy.JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY, "RSA");
        tls.put(JolokiaConnectionStrategy.JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY, "true");

        ConnectionUtils connectionUtils = createConnectionUtils(null, () -> tls);
        JolokiaConnectionStrategy strategy = JolokiaConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(8778)
                .build();

        // connect will fail because there's no real Jolokia server, but SSL properties should be set
        assertThatThrownBy(() -> strategy.connect(mockNode()))
                .isInstanceOf(IOException.class);

        assertThat(System.getProperty(JolokiaConnectionStrategy.JOLOKIA_CA_CERTIFICATE_PROPERTY)).isEqualTo("/ca.crt");
        assertThat(System.getProperty(JolokiaConnectionStrategy.JOLOKIA_CLIENT_CERTIFICATE_PROPERTY)).isEqualTo("/client.crt");
        assertThat(System.getProperty(JolokiaConnectionStrategy.JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY)).isEqualTo("/client.key");
        assertThat(System.getProperty(JolokiaConnectionStrategy.JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY)).isEqualTo("RSA");
        assertThat(System.getProperty(JolokiaConnectionStrategy.JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY)).isEqualTo("true");
    }

    @Test
    public void testJolokiaNoSslDoesNotSetProperties()
    {
        Map<String, String> tls = new HashMap<>();
        tls.put(JolokiaConnectionStrategy.ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY, "false");

        ConnectionUtils connectionUtils = createConnectionUtils(null, () -> tls);
        JolokiaConnectionStrategy strategy = JolokiaConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(8778)
                .build();

        assertThatThrownBy(() -> strategy.connect(mockNode()))
                .isInstanceOf(IOException.class);

        assertThat(System.getProperty(JolokiaConnectionStrategy.JOLOKIA_CA_CERTIFICATE_PROPERTY)).isNull();
    }

    @Test
    public void testRmiConnectSetsRmiTlsProperties()
    {
        Map<String, String> tls = new HashMap<>();
        tls.put("javax.net.ssl.keyStore", "/keystore.jks");
        tls.put("javax.net.ssl.keyStorePassword", "secret");

        ConnectionUtils connectionUtils = createConnectionUtils(null, () -> tls);
        RMIConnectionStrategy strategy = RMIConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(7199)
                .build();

        // connect will fail because there's no real JMX server, but TLS properties should be set
        assertThatThrownBy(() -> strategy.connect(mockNode()))
                .isInstanceOf(IOException.class);

        assertThat(System.getProperty("javax.net.ssl.keyStore")).isEqualTo("/keystore.jks");
        assertThat(System.getProperty("javax.net.ssl.keyStorePassword")).isEqualTo("secret");
    }

    @Test
    public void testRmiNoTlsDoesNotSetProperties()
    {
        ConnectionUtils connectionUtils = createConnectionUtils(null, HashMap::new);
        RMIConnectionStrategy strategy = RMIConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(7199)
                .build();

        assertThatThrownBy(() -> strategy.connect(mockNode()))
                .isInstanceOf(IOException.class);

        assertThat(System.getProperty("javax.net.ssl.keyStore")).isNull();
    }

    @Test
    public void testJolokiaWithCredentials()
    {
        String[] creds = {"user", "pass"};
        ConnectionUtils connectionUtils = createConnectionUtils(() -> creds, HashMap::new);
        JolokiaConnectionStrategy strategy = JolokiaConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(8778)
                .build();

        assertThat(connectionUtils.getCredentialsConfig()).containsExactly("user", "pass");
        assertThat(connectionUtils.isAuthEnabled()).isTrue();
    }

    @Test
    public void testRmiWithCredentials()
    {
        String[] creds = {"admin", "secret"};
        ConnectionUtils connectionUtils = createConnectionUtils(() -> creds, HashMap::new);
        RMIConnectionStrategy strategy = RMIConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .withPort(7199)
                .build();

        assertThat(connectionUtils.getCredentialsConfig()).containsExactly("admin", "secret");
        assertThat(connectionUtils.isAuthEnabled()).isTrue();
    }

    @Test
    public void testNoCredentialsNoTls()
    {
        ConnectionUtils connectionUtils = createConnectionUtils(null, HashMap::new);
        assertThat(connectionUtils.getCredentialsConfig()).isNull();
        assertThat(connectionUtils.isAuthEnabled()).isFalse();
        assertThat(connectionUtils.isTLSEnabled()).isFalse();
    }

    @Test
    public void testImplementsStrategy()
    {
        ConnectionUtils connectionUtils = createConnectionUtils(null, HashMap::new);

        JmxConnectionStrategy jolokia = JolokiaConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .build();
        JmxConnectionStrategy rmi = RMIConnectionStrategy.newBuilder()
                .withConnectionUtils(connectionUtils)
                .build();

        assertThat(jolokia).isInstanceOf(JmxConnectionStrategy.class);
        assertThat(rmi).isInstanceOf(JmxConnectionStrategy.class);
    }

    private ConnectionUtils createConnectionUtils(
            final java.util.function.Supplier<String[]> credentials,
            final java.util.function.Supplier<Map<String, String>> tls)
    {
        return ConnectionUtils.newBuilder()
                .withCredentials(credentials)
                .withTls(tls)
                .withIpTranslator(new IpTranslator())
                .build();
    }

    private Node mockNode()
    {
        return org.mockito.Mockito.mock(Node.class, invocation ->
        {
            if (invocation.getMethod().getName().equals("getBroadcastRpcAddress"))
            {
                java.net.InetSocketAddress address = new java.net.InetSocketAddress("127.0.0.1", 9042);
                return java.util.Optional.of(address);
            }
            if (invocation.getMethod().getName().equals("getListenAddress"))
            {
                java.net.InetSocketAddress address = new java.net.InetSocketAddress("127.0.0.1", 7000);
                return java.util.Optional.of(address);
            }
            return org.mockito.Mockito.RETURNS_DEFAULTS.answer(invocation);
        });
    }
}
