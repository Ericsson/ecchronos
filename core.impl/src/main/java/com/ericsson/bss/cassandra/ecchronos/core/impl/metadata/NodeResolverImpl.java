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
package com.ericsson.bss.cassandra.ecchronos.core.impl.metadata;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;

public class NodeResolverImpl implements NodeResolver
{
    private static final int MAX_CACHE_SIZE = 500;
    private static final long EXPIRE_MINUTES = 60;

    private final Cache<InetAddress, DriverNode> addressToNodeMap = Caffeine.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .expireAfterAccess(EXPIRE_MINUTES, TimeUnit.MINUTES)
            .build();
    private final Cache<UUID, DriverNode> idToNodeMap = Caffeine.newBuilder()
            .maximumSize(MAX_CACHE_SIZE)
            .expireAfterAccess(EXPIRE_MINUTES, TimeUnit.MINUTES)
            .build();

    private final CqlSession session;

    public NodeResolverImpl(final CqlSession aSession)
    {
        this.session = aSession;
    }

    @Override
    public final Optional<DriverNode> fromIp(final InetAddress inetAddress)
    {
        DriverNode node = addressToNodeMap.getIfPresent(inetAddress);

        if (node == null)
        {
            node = lookup(inetAddress);
            if (node != null)
            {
                addressToNodeMap.put(inetAddress, node);
            }
        }
        else if (!inetAddress.equals(node.getPublicAddress()))
        {
            addressToNodeMap.invalidate(inetAddress);
            return fromIp(inetAddress);
        }

        return Optional.ofNullable(node);
    }

    @Override
    public final Optional<DriverNode> fromUUID(final UUID nodeId)
    {
        return Optional.ofNullable(resolve(nodeId));
    }

    private DriverNode resolve(final UUID nodeId)
    {
        DriverNode node = idToNodeMap.getIfPresent(nodeId);
        if (node == null)
        {
            node = lookup(nodeId);
            if (node != null)
            {
                idToNodeMap.put(nodeId, node);
            }
        }

        return node;
    }

    private DriverNode lookup(final UUID nodeId)
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

    private DriverNode lookup(final InetAddress inetAddress)
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
