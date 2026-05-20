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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.utils.dns.ReverseDNS;

import java.net.UnknownHostException;

public final class JmxHostResolver
{
    public static final String NO_BROADCAST_ADDRESS = "0.0.0.0"; //NOPMD AvoidUsingHardCodedIP

    private final IpTranslator myIpTranslator;
    private final boolean myReverseDNSResolution;

    public JmxHostResolver(final IpTranslator ipTranslator, final boolean reverseDNSResolution)
    {
        myIpTranslator = ipTranslator;
        myReverseDNSResolution = reverseDNSResolution;
    }

    /**
     * Resolve the JMX host address for a node, applying IP translation, reverse DNS, and IPv6 formatting.
     */
    public String resolve(final Node node) throws UnknownHostException
    {
        String host = node.getBroadcastRpcAddress().get().getAddress().getHostAddress();
        if (NO_BROADCAST_ADDRESS.equals(host))
        {
            host = node.getListenAddress().get().getHostString();
        }
        if (myIpTranslator.isActive())
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
