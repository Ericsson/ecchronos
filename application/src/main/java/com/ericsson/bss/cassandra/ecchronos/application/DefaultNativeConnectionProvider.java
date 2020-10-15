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

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalNativeConnectionProvider;

public class DefaultNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNativeConnectionProvider.class);

    private final LocalNativeConnectionProvider myLocalNativeConnectionProvider;

    public DefaultNativeConnectionProvider(Config config, Supplier<Security.CqlSecurity> cqlSecuritySupplier)
    {
        Config.NativeConnection nativeConfig = config.getConnectionConfig().getCql();
        String host = nativeConfig.getHost();
        int port = nativeConfig.getPort();
        Security.CqlSecurity cqlSecurity = cqlSecuritySupplier.get();
        boolean authEnabled = cqlSecurity.getCredentials().isEnabled();
        boolean tlsEnabled = cqlSecurity.getTls().isEnabled();
        LOG.info("Connecting through CQL using {}:{}, authentication: {}, tls: {}", host, port, authEnabled,
                tlsEnabled);

        ExtendedAuthProvider authProvider = new ReloadingAuthProvider(() -> cqlSecuritySupplier.get().getCredentials());

        SSLOptions sslOptions = null;
        if (tlsEnabled)
        {
            sslOptions = new ReloadingCertificateHandler(() -> cqlSecuritySupplier.get().getTls());
        }

        LocalNativeConnectionProvider.Builder nativeConnectionBuilder = LocalNativeConnectionProvider.builder()
                .withLocalhost(host)
                .withPort(port)
                .withAuthProvider(authProvider)
                .withSslOptions(sslOptions);
        establishTemporaryConnection(LocalNativeConnectionProvider.Builder.fromBuilder(nativeConnectionBuilder).build(),
                host, port, nativeConfig.getTimeout());

        myLocalNativeConnectionProvider = nativeConnectionBuilder
                .build();
    }

    private static Session establishTemporaryConnection(Cluster cluster, String host, int port, long timeout)
    {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + timeout;
        while (endTime > System.currentTimeMillis())
        {
            try
            {
                return cluster.connect();
            }
            catch (NoHostAvailableException | IllegalStateException e)
            {
                try
                {
                    LOG.warn("Unable to connect through CQL using {}:{}, retrying", host, port);
                    LOG.debug("Connection failed, retrying in {}", timeout, e);
                    Thread.sleep(5000);
                }
                catch (InterruptedException e1)
                {
                    // Should never occur
                    throw new RuntimeException("Unexpected interrupt", e); //NOPMD
                }
            }
        }
        return cluster.connect();
    }

    @Override
    public Session getSession()
    {
        return myLocalNativeConnectionProvider.getSession();
    }

    @Override
    public Host getLocalHost()
    {
        return myLocalNativeConnectionProvider.getLocalHost();
    }

    @Override
    public void close()
    {
        myLocalNativeConnectionProvider.close();
    }
}
