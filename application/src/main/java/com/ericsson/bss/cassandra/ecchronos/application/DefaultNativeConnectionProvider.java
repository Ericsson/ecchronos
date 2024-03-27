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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.ericsson.bss.cassandra.ecchronos.application.config.connection.NativeConnection;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.RetryPolicy;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
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
                                           final MeterRegistry meterRegistry)
    {
        NativeConnection nativeConfig = config.getConnectionConfig().getCqlConnection();
        String host = nativeConfig.getHost();
        int port = nativeConfig.getPort();
        boolean remoteRouting = nativeConfig.getRemoteRouting();
        Security.CqlSecurity cqlSecurity = cqlSecuritySupplier.get();
        boolean authEnabled = cqlSecurity.getCqlCredentials().isEnabled();
        boolean tlsEnabled = cqlSecurity.getCqlTlsConfig().isEnabled();
        LOG.info("Connecting through CQL using {}:{}, authentication: {}, tls: {}", host, port, authEnabled,
                tlsEnabled);
        AuthProvider authProvider = null;
        if (authEnabled)
        {
            authProvider = new ReloadingAuthProvider(() -> cqlSecuritySupplier.get().getCqlCredentials());
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
                .withMetricsEnabled(config.getStatisticsConfig().isEnabled())
                .withMeterRegistry(meterRegistry)
                .withSchemaChangeListener(defaultRepairConfigurationProvider)
                .withNodeStateListener(defaultRepairConfigurationProvider);

        myLocalNativeConnectionProvider = establishConnection(nativeConnectionBuilder,
                host, port, nativeConfig.getTimeout().getConnectionTimeout(TimeUnit.MILLISECONDS),
                nativeConfig.getRetryPolicy());
    }

    public DefaultNativeConnectionProvider(final Config config,
                                           final Supplier<Security.CqlSecurity> cqlSecuritySupplier,
                                           final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
                                           final MeterRegistry meterRegistry)
    {
        this(config, cqlSecuritySupplier,
                new ReloadingCertificateHandler(() -> cqlSecuritySupplier.get().getCqlTlsConfig()),
                defaultRepairConfigurationProvider, meterRegistry);
    }

    private static LocalNativeConnectionProvider establishConnection(
            final LocalNativeConnectionProvider.Builder builder,
            final String host,
            final int port,
            final long timeout,
            final RetryPolicy retryPolicy)
    {
        for (int count = 1; count <= retryPolicy.getMaxAttempts(); count++)
        {
            try
            {
                return builder.build();
            }
            catch (AllNodesFailedException | IllegalStateException e)
            {
                LOG.warn("Unable to connect through CQL using {}:{}, retrying", host, port);
                long delay = retryPolicy.currentDelay(count);
                long currentTime = System.currentTimeMillis();
                long endTime = currentTime + timeout;
                try
                {
                    if (currentTime + delay > endTime)
                    {
                        String msg = String.format(
                                "Connection failed in attempt %d of %d. Retrying in 5 seconds.",
                                count, retryPolicy.getMaxAttempts());
                        LOG.warn(msg);
                        Thread.sleep(SLEEP_TIME);
                    }
                    else
                    {
                        String msg = String.format(
                                "Connection failed in attempt %d of %d. Retrying in %d seconds.",
                                count, retryPolicy.getMaxAttempts(), TimeUnit.MILLISECONDS.toSeconds(delay));
                        LOG.warn(msg);
                        Thread.sleep(delay);
                    }
                }
                catch (InterruptedException e1)
                {
                    LOG.error("Unexpected interrupt while trying to connect to Cassandra", e1);
                    throw new RuntimeException(e1);
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
