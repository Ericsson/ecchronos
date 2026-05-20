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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedJmxBuilder.ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY;
import static com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedJmxBuilder.JOLOKIA_CA_CERTIFICATE_PROPERTY;
import static com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedJmxBuilder.JOLOKIA_CLIENT_CERTIFICATE_PROPERTY;
import static com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedJmxBuilder.JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY;
import static com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedJmxBuilder.JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY;
import static com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.DistributedJmxBuilder.JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY;

public final class JmxEnvironmentFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(JmxEnvironmentFactory.class);

    private JmxEnvironmentFactory()
    {
    }

    /**
     * Create the JMX environment map with credentials and TLS configuration.
     */
    public static Map<String, Object> create(final Supplier<String[]> credentialsSupplier,
                                             final Supplier<Map<String, String>> tlsSupplier,
                                             final boolean jolokiaEnabled)
    {
        Map<String, Object> env = new HashMap<>();
        String[] credentials = credentialsSupplier != null ? credentialsSupplier.get() : null;
        Map<String, String> tls = tlsSupplier != null ? tlsSupplier.get() : new HashMap<>();

        if (credentials != null)
        {
            env.put(JMXConnector.CREDENTIALS, credentials);
        }

        if (jolokiaEnabled && String.valueOf(true).equals(tls.get(ECCHRONOS_JOLOKIA_SSL_ENABLED_PROPERTY)))
        {
            configureJolokiaSsl(tls);
        }
        else if (!tls.isEmpty())
        {
            configureRmiTls(env, tls);
        }
        return env;
    }

    private static void configureJolokiaSsl(final Map<String, String> tls)
    {
        LOG.info("Setting Jolokia client with PEM certificates");
        setSystemPropertyIfNotNull(JOLOKIA_CA_CERTIFICATE_PROPERTY, tls.get(JOLOKIA_CA_CERTIFICATE_PROPERTY));
        setSystemPropertyIfNotNull(JOLOKIA_CLIENT_CERTIFICATE_PROPERTY, tls.get(JOLOKIA_CLIENT_CERTIFICATE_PROPERTY));
        setSystemPropertyIfNotNull(JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY, tls.get(JOLOKIA_CLIENT_KEY_CERTIFICATE_PROPERTY));
        setSystemPropertyIfNotNull(JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY, tls.get(JOLOKIA_CLIENT_KEY_ALGORITHM_CERTIFICATE_PROPERTY));
        setSystemPropertyIfNotNull(JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY, tls.get(JDK_DISABLE_HOSTNAME_VERIFICATION_PROPERTY));
    }

    private static void configureRmiTls(final Map<String, Object> env, final Map<String, String> tls)
    {
        for (Map.Entry<String, String> configEntry : tls.entrySet())
        {
            String key = configEntry.getKey();
            String value = configEntry.getValue();
            if (value != null && !value.isEmpty())
            {
                System.setProperty(key, value);
            }
            else
            {
                System.clearProperty(key);
            }
        }
        env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
    }

    private static void setSystemPropertyIfNotNull(final String key, final String value)
    {
        if (value != null)
        {
            System.setProperty(key, value);
        }
    }
}
