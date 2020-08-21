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
package com.ericsson.bss.cassandra.ecchronos.core.osgi;

import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;

import org.osgi.service.component.annotations.*;

import com.datastax.driver.core.Metadata;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;

@Component(service = NodeResolver.class)
public class NodeResolverService implements NodeResolver
{
    @Reference(service = NativeConnectionProvider.class, cardinality = ReferenceCardinality.MANDATORY, policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider myNativeConnectionProvider;

    private volatile NodeResolver delegateNodeResolver;

    @Activate
    public void activate()
    {
        Metadata metadata = myNativeConnectionProvider.getSession().getCluster().getMetadata();

        delegateNodeResolver = new NodeResolverImpl(metadata);
    }

    @Override
    public Optional<Node> fromIp(InetAddress inetAddress)
    {
        return delegateNodeResolver.fromIp(inetAddress);
    }

    @Override
    public Optional<Node> fromUUID(UUID nodeId)
    {
        return delegateNodeResolver.fromUUID(nodeId);
    }
}
