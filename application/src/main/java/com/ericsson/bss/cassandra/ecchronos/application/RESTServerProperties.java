/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import java.net.InetSocketAddress;
import java.util.Properties;

public final class RESTServerProperties
{
    private static final String CONFIG_HOST = "rest.host";
    private static final String CONFIG_PORT = "rest.port";

    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_PORT = "8080";

    private final InetSocketAddress myAddress;

    private RESTServerProperties(InetSocketAddress address)
    {
        myAddress = address;
    }

    public InetSocketAddress getAddress()
    {
        return myAddress;
    }

    @Override
    public String toString()
    {
        return String.format("(address=%s)", myAddress);
    }

    public static RESTServerProperties from(Properties properties)
    {
        String host = properties.getProperty(CONFIG_HOST, DEFAULT_HOST);
        int port = Integer.parseInt(properties.getProperty(CONFIG_PORT, DEFAULT_PORT));

        InetSocketAddress address = new InetSocketAddress(host, port);

        return new RESTServerProperties(address);
    }
}
