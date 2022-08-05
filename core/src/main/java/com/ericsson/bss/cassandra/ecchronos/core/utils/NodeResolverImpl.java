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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;

public class NodeResolverImpl implements NodeResolver
{
    private final ConcurrentMap<InetAddress, DriverNode> addressToNodeMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<UUID, DriverNode> idToNodeMap = new ConcurrentHashMap<>();

    private final CqlSession session;

    public NodeResolverImpl(CqlSession session)
    {
        this.session = session;
    }

    @Override
    public Optional<DriverNode> fromIp(InetAddress inetAddress)
    {
        DriverNode node = addressToNodeMap.get(inetAddress);

        if (node == null)
        {
            node = addressToNodeMap.computeIfAbsent(inetAddress, address -> lookup(inetAddress));
        }
        else if (!inetAddress.equals(node.getPublicAddress()))
        {
            // IP mapping is wrong, we should remove the old entry and retry
            addressToNodeMap.remove(inetAddress, node);
            return fromIp(inetAddress);
        }

        return Optional.ofNullable(node);
    }

    @Override
    public Optional<DriverNode> fromUUID(UUID nodeId)
    {
        return Optional.ofNullable(resolve(nodeId));
    }

    private DriverNode resolve(UUID nodeId)
    {
        DriverNode node = idToNodeMap.get(nodeId);
        if (node == null)
        {
            node = idToNodeMap.computeIfAbsent(nodeId, this::lookup);
        }

        return node;
    }

    private DriverNode lookup(UUID nodeId)
    {
        Metadata metadata = session.getMetadata();
        for (Node node : metadata.getNodes().values())
        {
            if (node.getHostId().equals(nodeId))
            {
                return new DriverNode(node);
            }
        }
        return null;
    }

    private DriverNode lookup(InetAddress inetAddress)
    {
        Metadata metadata = session.getMetadata();
        for (Node node : metadata.getNodes().values())
        {
            if (node.getBroadcastAddress().get().getAddress().equals(inetAddress))
            {
                return resolve(node.getHostId());
            }
        }
        return null;
    }
}
