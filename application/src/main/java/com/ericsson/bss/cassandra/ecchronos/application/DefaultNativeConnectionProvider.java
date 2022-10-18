/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalNativeConnectionProvider;

public class DefaultNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNativeConnectionProvider.class);

    private static final int SLEEP_TIME = 5000;

    private final LocalNativeConnectionProvider myLocalNativeConnectionProvider;

    public DefaultNativeConnectionProvider(final Config config,
                                           final Supplier<Security.CqlSecurity> cqlSecuritySupplier,
                                           final CertificateHandler certificateHandler,
                                           final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
                                           final MetricRegistry metricRegistry)
    {
        Config.NativeConnection nativeConfig = config.getConnectionConfig().getCql();
        String host = nativeConfig.getHost();
        int port = nativeConfig.getPort();
        boolean remoteRouting = nativeConfig.getRemoteRouting();
        Security.CqlSecurity cqlSecurity = cqlSecuritySupplier.get();
        boolean authEnabled = cqlSecurity.getCredentials().isEnabled();
        boolean tlsEnabled = cqlSecurity.getTls().isEnabled();
        LOG.info("Connecting through CQL using {}:{}, authentication: {}, tls: {}", host, port, authEnabled,
                tlsEnabled);
        AuthProvider authProvider = null;
        if (authEnabled)
        {
            authProvider = new ReloadingAuthProvider(() -> cqlSecuritySupplier.get().getCredentials());
        }

        SslEngineFactory sslEngineFactory = null;
        if (tlsEnabled)
        {
            sslEngineFactory = certificateHandler;
        }

        LocalNativeConnectionProvider.Builder nativeConnectionBuilder = LocalNativeConnectionProvider.builder()
                .withLocalhost(host)
                .withPort(port)
                .withRemoteRouting(remoteRouting)
                .withAuthProvider(authProvider)
                .withSslEngineFactory(sslEngineFactory)
                .withMetricRegistry(metricRegistry)
                .withMetricsEnabled(config.getStatistics().isEnabled())
                .withSchemaChangeListener(defaultRepairConfigurationProvider);

        myLocalNativeConnectionProvider = establishConnection(nativeConnectionBuilder,
                host, port, nativeConfig.getTimeout().getConnectionTimeout(TimeUnit.MILLISECONDS));
    }

    public DefaultNativeConnectionProvider(final Config config,
                                           final Supplier<Security.CqlSecurity> cqlSecuritySupplier,
                                           final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
                                           final MetricRegistry metricRegistry)
    {
        this(config, cqlSecuritySupplier, new ReloadingCertificateHandler(() -> cqlSecuritySupplier.get().getTls()),
                defaultRepairConfigurationProvider, metricRegistry);
    }

    private static LocalNativeConnectionProvider establishConnection(
            final LocalNativeConnectionProvider.Builder builder,
            final String host,
            final int port,
            final long timeout)
    {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeout;
        while (endTime > System.currentTimeMillis())
        {
            try
            {
                return builder.build();
            }
            catch (AllNodesFailedException | IllegalStateException e)
            {
                try
                {
                    LOG.warn("Unable to connect through CQL using {}:{}, retrying", host, port);
                    if (timeout != 0)
                    {
                        LOG.debug("Connection failed, retrying in 5 seconds", e);
                        Thread.sleep(SLEEP_TIME);
                    }
                }
                catch (InterruptedException e1)
                {
                    // Should never occur
                    throw new RuntimeException("Unexpected interrupt", e); // NOPMD
                }
            }
        }
        return builder.build();
    }

    @Override
    public final CqlSession getSession()
    {
        return myLocalNativeConnectionProvider.getSession();
    }

    @Override
    public final Node getLocalNode()
    {
        return myLocalNativeConnectionProvider.getLocalNode();
    }

    @Override
    public final boolean getRemoteRouting()
    {
        return myLocalNativeConnectionProvider.getRemoteRouting();
    }

    @Override
    public final void close()
    {
        myLocalNativeConnectionProvider.close();
    }
}
