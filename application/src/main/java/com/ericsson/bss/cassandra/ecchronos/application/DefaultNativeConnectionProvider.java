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

import com.datastax.driver.core.ExtendedAuthProvider;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
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
        LOG.info("Connecting through CQL using {}:{}, authentication: {}, tls: {}", host, port, enabled(authEnabled),
                enabled(tlsEnabled));

        ExtendedAuthProvider authProvider = new ReloadingAuthProvider(() -> cqlSecuritySupplier.get().getCredentials());

        SSLOptions sslOptions = null;
        if (tlsEnabled)
        {
            sslOptions = new ReloadingCertificateHandler(() -> cqlSecuritySupplier.get().getTls());
        }

        myLocalNativeConnectionProvider = LocalNativeConnectionProvider.builder()
                .withLocalhost(host)
                .withPort(port)
                .withAuthProvider(authProvider)
                .withSslOptions(sslOptions)
                .build();
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

    private static String enabled(boolean enabled)
    {
        return enabled ? "enabled" : "disabled";
    }
}
