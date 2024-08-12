/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.metadata.EndPoint;

import java.net.InetSocketAddress;
import java.util.Objects;

public class ContactEndPoint implements EndPoint
{
    private final String hostName;
    private final String metricPrefix;
    private final int port;
    private volatile InetSocketAddress lastResolvedAddress;

    public ContactEndPoint(final String aHostName, final int aPort)
    {
        this.hostName = aHostName;
        this.port = aPort;
        this.metricPrefix = buildMetricPrefix(aHostName, aPort);
    }

    @Override
    public final InetSocketAddress resolve()
    {
        lastResolvedAddress = new InetSocketAddress(hostName, port);
        return lastResolvedAddress;
    }

    @Override
    public final String asMetricPrefix()
    {
        return metricPrefix;
    }

    @Override
    public final boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ContactEndPoint that = (ContactEndPoint) o;
        return port == that.port && hostName.equals(that.hostName);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(hostName, port);
    }

    @Override
    public final String toString()
    {
        if (lastResolvedAddress != null)
        {
            return lastResolvedAddress.toString();
        }
        return String.format("%s:%d", hostName, port);
    }

    private static String buildMetricPrefix(final String host, final int port)
    {
        return host.replace('.', '_') + ':' + port;
    }
}

