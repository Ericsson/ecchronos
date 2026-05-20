/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils;

import java.net.UnknownHostException;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.utils.dns.ReverseDNS;

/**
 * Resolves the JMX host address for a Cassandra node.
 * Handles IP translation, reverse DNS resolution, and IPv6 formatting.
 */
public final class JmxHostResolver
{
    private static final String NO_BROADCAST_ADDRESS = "0.0.0.0"; //NOPMD AvoidUsingHardCodedIP

    private final IpTranslator myIpTranslator;
    private final boolean myReverseDNSResolution;

    /**
     * Constructs a JmxHostResolver.
     *
     * @param ipTranslator the IP translator for address mapping.
     * @param reverseDNSResolution whether to perform reverse DNS lookup.
     */
    public JmxHostResolver(final IpTranslator ipTranslator, final boolean reverseDNSResolution)
    {
        myIpTranslator = ipTranslator;
        myReverseDNSResolution = reverseDNSResolution;
    }

    /**
     * Resolves the JMX host address for the given node.
     * Applies IP translation, reverse DNS, and IPv6 bracket formatting as configured.
     *
     * @param node the Cassandra node to resolve.
     * @return the resolved host string.
     * @throws UnknownHostException if the host cannot be resolved.
     */
    public String resolve(final Node node) throws UnknownHostException
    {
        String host = node.getBroadcastRpcAddress().get().getAddress().getHostAddress();
        if (NO_BROADCAST_ADDRESS.equals(host))
        {
            host = node.getListenAddress().get().getHostString();
        }
        if (myIpTranslator != null && myIpTranslator.isActive())
        {
            host = myIpTranslator.getInternalIp(host);
        }
        if (myReverseDNSResolution)
        {
            host = ReverseDNS.fromHostString(host);
        }
        if (host.contains(":") && !host.startsWith("[") && !host.endsWith("]"))
        {
            host = "[" + host + "]";
        }
        return host;
    }
}
