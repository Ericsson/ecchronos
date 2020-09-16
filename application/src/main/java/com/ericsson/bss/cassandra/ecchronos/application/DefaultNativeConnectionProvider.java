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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.LocalNativeConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(DefaultNativeConnectionProvider.class);

    private final LocalNativeConnectionProvider myLocalNativeConnectionProvider;

    public DefaultNativeConnectionProvider(Config config)
    {
        Config.NativeConnection nativeConfig = config.getConnectionConfig().getCql();
        String host = nativeConfig.getHost();
        int port = nativeConfig.getPort();
        LOG.info("Connecting through CQL using {}:{}", host, port);

        myLocalNativeConnectionProvider = LocalNativeConnectionProvider.builder()
                .withLocalhost(host)
                .withPort(port)
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
}
