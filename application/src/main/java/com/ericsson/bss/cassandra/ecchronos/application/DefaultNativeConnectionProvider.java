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
import com.ericsson.bss.cassandra.ecchronos.application.config.exceptions.RetryPolicyException;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.state.ApplicationStateHolder;
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

    private final LocalNativeConnectionProvider myLocalNativeConnectionProvider;
    public DefaultNativeConnectionProvider(final Config config,
                                           final Supplier<Security.CqlSecurity> cqlSecuritySupplier,
                                           final CertificateHandler certificateHandler,
                                           final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
                                           final MeterRegistry meterRegistry)
    {
        NativeConnection nativeConfig = config.getConnectionConfig().getCqlConnection();
        String repairHistoryKeyspace = config.getRepairConfig().getRepairHistory().getKeyspaceName();
        String host = nativeConfig.getHost();
        int port = nativeConfig.getPort();
        boolean remoteRouting = nativeConfig.getRemoteRouting();
        Security.CqlSecurity cqlSecurity = cqlSecuritySupplier.get();
        boolean authEnabled = cqlSecurity.getCqlCredentials().isEnabled();
        boolean tlsEnabled = cqlSecurity.getCqlTlsConfig().isEnabled();
        LOG.info("Connecting through CQL using {}:{}, authentication: {}, tls: {}",
                host,
                port,
                authEnabled,
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
                .withNodeStateListener(defaultRepairConfigurationProvider)
                .withRepairHistoryKeyspace(repairHistoryKeyspace);

        myLocalNativeConnectionProvider = establishConnection(nativeConnectionBuilder,
                host, port, nativeConfig.getTimeout().getConnectionTimeout(TimeUnit.MILLISECONDS),
                nativeConfig.getRetryPolicy());

        ApplicationStateHolder.getInstance().put("connections.cql." + host + ".port", port);
        ApplicationStateHolder.getInstance().put("connections.cql." + host + ".authentication", authEnabled);
        ApplicationStateHolder.getInstance().put("connections.cql." + host + ".tls", tlsEnabled);
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
        for (int attempt = 1; attempt <= retryPolicy.getMaxAttempts(); attempt++)
        {
            try
            {
                return tryEstablishConnection(builder);
            }
            catch (AllNodesFailedException | IllegalStateException e)
            {
                handleRetry(attempt, retryPolicy, host, port, timeout);
            }
        }
        throw new RetryPolicyException("Failed to establish connection after all retry attempts.");
    }

    private static LocalNativeConnectionProvider tryEstablishConnection(
        final LocalNativeConnectionProvider.Builder builder)
    {
        try
        {
            return builder.build();
        }
        catch (AllNodesFailedException | IllegalStateException e)
        {
            LOG.error("Unexpected interrupt while trying to connect to Cassandra. Reason: ", e);
            throw e;
        }
    }

    private static void handleRetry(
        final int attempt,
        final RetryPolicy retryPolicy,
        final String host,
        final int port,
        final long timeout)
    {
        LOG.warn("Unable to connect through CQL using {}:{}.", host, port);
        long delay = retryPolicy.currentDelay(attempt);
        long currentTime = System.currentTimeMillis();
        long endTime = currentTime + timeout;
        if (currentTime + delay > endTime)
        {
            delay = timeout;
        }

        if (attempt == retryPolicy.getMaxAttempts())
        {
            LOG.error("All connection attempts failed ({} were made)!", attempt);
        }
        else
        {
            LOG.warn("Connection attempt {} of {} failed. Retrying in {} seconds.",
                    attempt,
                    retryPolicy.getMaxAttempts(),
                    TimeUnit.MILLISECONDS.toSeconds(delay));
            try
            {
                Thread.sleep(delay);
            }
            catch (InterruptedException ie)
            {
                LOG.error("Exception caught during the delay time, while trying to reconnect to Cassandra.", ie);
            }
        }

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
