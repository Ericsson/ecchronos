/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.connection.impl;

import com.datastax.driver.core.EndPoint;

import java.net.InetSocketAddress;
import java.util.Objects;

public class ContactEndPoint implements EndPoint
{
    private final String hostName;
    private final int port;
    private volatile InetSocketAddress lastResolvedAddress;

    public ContactEndPoint(String hostName, int port)
    {
        this.hostName = hostName;
        this.port = port;
    }

    @Override
    public InetSocketAddress resolve()
    {
        lastResolvedAddress = new InetSocketAddress(hostName, port);
        return lastResolvedAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContactEndPoint that = (ContactEndPoint) o;
        return port == that.port && hostName.equals(that.hostName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostName, port);
    }

    @Override
    public String toString()
    {
        if (lastResolvedAddress != null)
        {
            return lastResolvedAddress.toString();
        }
        return String.format("%s:%d", hostName, port);
    }
}
