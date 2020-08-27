/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import java.net.InetAddress;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class NodeResolverImpl implements NodeResolver
{
    private final ConcurrentMap<InetAddress, DriverNode> addressToHostMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, DriverNode> idToHostMap = new ConcurrentHashMap<>();

    private final Metadata metadata;

    public NodeResolverImpl(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Optional<Node> fromIp(InetAddress inetAddress)
    {
        DriverNode node = addressToHostMap.get(inetAddress);

        if (node == null)
        {
            node = addressToHostMap.computeIfAbsent(inetAddress, address -> lookup(inetAddress));
        }
        else if (!inetAddress.equals(node.getPublicAddress()))
        {
            // IP mapping is wrong, we should remove the old entry and retry
            addressToHostMap.remove(inetAddress, node);
            return fromIp(inetAddress);
        }

        return Optional.ofNullable(node);
    }

    private DriverNode resolve(UUID nodeId)
    {
        DriverNode node = idToHostMap.get(nodeId);
        if (node == null)
        {
            node = idToHostMap.computeIfAbsent(nodeId, this::lookup);
        }

        return node;
    }

    private DriverNode lookup(UUID nodeId)
    {
        for (Host host : metadata.getAllHosts())
        {
            if (host.getHostId().equals(nodeId))
            {
                return new DriverNode(host);
            }
        }
        return null;
    }

    private DriverNode lookup(InetAddress inetAddress)
    {
        for (Host host : metadata.getAllHosts())
        {
            if (host.getBroadcastAddress().equals(inetAddress))
            {
                return resolve(host.getHostId());
            }
        }
        return null;
    }

    private class DriverNode implements Node
    {
        private final Host host;

        public DriverNode(Host host)
        {
            this.host = host;
        }

        @Override
        public UUID getId()
        {
            return host.getHostId();
        }

        @Override
        public InetAddress getPublicAddress()
        {
            return host.getBroadcastAddress();
        }

        @Override
        public String getDatacenter()
        {
            return host.getDatacenter();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            DriverNode that = (DriverNode) o;
            return host.equals(that.host);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(host);
        }

        @Override
        public String toString()
        {
            return String.format("Node(%s:%s:%s)", getId(), getDatacenter(), getPublicAddress());
        }
    }
}
