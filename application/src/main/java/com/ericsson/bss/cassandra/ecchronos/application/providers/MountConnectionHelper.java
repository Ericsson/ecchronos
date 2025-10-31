/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.providers;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.AgentConnectionConfig;

public class MountConnectionHelper
{
    /**
     * Resolves the initial contact points from the provided map of host configurations.
     *
     * @param contactPoints
     *         a map containing the host configurations.
     * @return a list of {@link InetSocketAddress} representing the resolved contact points.
     */
    public final List<InetSocketAddress> resolveInitialContactPoints(
            final Map<String, AgentConnectionConfig.Host> contactPoints)
    {
        List<InetSocketAddress> resolvedContactPoints = new ArrayList<>();
        for (AgentConnectionConfig.Host host : contactPoints.values())
        {
            InetSocketAddress tmpAddress = InetSocketAddress.createUnresolved(host.getHost(), host.getPort());
            resolvedContactPoints.add(tmpAddress);
        }
        return resolvedContactPoints;
    }

    /**
     * Resolves the datacenter-aware configuration from the specified {@link AgentConnectionConfig.DatacenterAware}
     * object.
     *
     * @param datacenterAware
     *         the datacenter-aware configuration object.
     * @return a list of datacenter names.
     */
    public final List<String> resolveDatacenterAware(final AgentConnectionConfig.DatacenterAware datacenterAware)
    {
        List<String> datacenterNames = new ArrayList<>();
        for (AgentConnectionConfig.DatacenterAware.Datacenter datacenter : datacenterAware.getDatacenters().values())
        {
            datacenterNames.add(datacenter.getName());
        }
        return datacenterNames;
    }

    /**
     * Resolves the rack-aware configuration from the specified {@link AgentConnectionConfig.RackAware} object.
     *
     * @param rackAware
     *         the rack-aware configuration object.
     * @return a list of maps containing datacenter and rack information.
     */
    public final List<Map<String, String>> resolveRackAware(final AgentConnectionConfig.RackAware rackAware)
    {
        List<Map<String, String>> rackList = new ArrayList<>();
        for (AgentConnectionConfig.RackAware.Rack rack : rackAware.getRacks().values())
        {
            Map<String, String> rackInfo = new HashMap<>();
            rackInfo.put("datacenterName", rack.getDatacenterName());
            rackInfo.put("rackName", rack.getRackName());
            rackList.add(rackInfo);
        }
        return rackList;
    }

    /**
     * Resolves the host-aware configuration from the specified {@link AgentConnectionConfig.HostAware} object.
     *
     * @param hostAware
     *         the host-aware configuration object.
     * @return a list of {@link InetSocketAddress} representing the resolved hosts.
     */
    public final List<InetSocketAddress> resolveHostAware(final AgentConnectionConfig.HostAware hostAware)
    {
        List<InetSocketAddress> resolvedHosts = new ArrayList<>();
        for (AgentConnectionConfig.Host host : hostAware.getHosts().values())
        {
            InetSocketAddress tmpAddress = new InetSocketAddress(host.getHost(), host.getPort());
            resolvedHosts.add(tmpAddress);
        }
        return resolvedHosts;
    }
}
