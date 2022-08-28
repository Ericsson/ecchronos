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

import com.datastax.oss.driver.api.core.CqlSession;
import org.osgi.service.component.annotations.*;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
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
        CqlSession session = myNativeConnectionProvider.getSession();

        delegateNodeResolver = new NodeResolverImpl(session);
    }

    @Override
    public Optional<DriverNode> fromIp(InetAddress inetAddress)
    {
        return delegateNodeResolver.fromIp(inetAddress);
    }

    @Override
    public Optional<DriverNode> fromUUID(UUID nodeId)
    {
        return delegateNodeResolver.fromUUID(nodeId);
    }
}
