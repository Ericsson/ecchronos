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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.impl.logging.ThrottlingLogger;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.state.HostStates;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.metadata.Node;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link HostStates} interface using JMX to retrieve node statuses and then caches the retrieved
 * statuses for some time.
 */
public final class HostStatesImpl implements HostStates, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(HostStatesImpl.class);
    private static final ThrottlingLogger THROTTLED_LOGGER = new ThrottlingLogger(LOG, 1, TimeUnit.MINUTES);

    private static final long DEFAULT_REFRESH_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(10);

    private final ConcurrentHashMap<InetAddress, Boolean> myHostStates = new ConcurrentHashMap<>();
    private final Object myRefreshLock = new Object();
    private final long myRefreshIntervalInMs;

    private volatile long myLastRefresh = -1;

    private final DistributedJmxProxyFactory myJmxProxyFactory;

    private HostStatesImpl(final Builder builder)
    {
        myRefreshIntervalInMs = builder.myRefreshIntervalInMs;
        myJmxProxyFactory = builder.myJmxProxyFactory;
    }

    @Override
    public boolean isUp(final Node node)
    {
        refreshNodeStatus(node.getHostId());
        Boolean status = myHostStates.get(node.getBroadcastAddress().get().getAddress());
        return status != null && status;
    }

    @Override
    public boolean isUp(final DriverNode node)
    {
        refreshNodeStatus(node.getId());
        Boolean status = myHostStates.get(node.getPublicAddress());
        return status != null && status;
    }

    @Override
    public void close()
    {
        myHostStates.clear();
    }

    private void refreshNodeStatus(final UUID nodeID)
    {
        if (shouldRefreshNodeStatus())
        {
            synchronized (myRefreshLock)
            {
                if (shouldRefreshNodeStatus() && !tryRefreshHostStates(nodeID))
                {
                    myHostStates.clear();
                }
            }
        }
    }

    @VisibleForTesting
    void resetLastRefresh()
    {
        myLastRefresh = -1;
    }

    private boolean shouldRefreshNodeStatus()
    {
        return myLastRefresh == -1 || myLastRefresh < (System.currentTimeMillis() - myRefreshIntervalInMs);
    }

    private synchronized boolean tryRefreshHostStates(final UUID nodeID)
    {
        if (myJmxProxyFactory == null)
        {
            return false;
        }

        try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
        {
            for (String liveHost : proxy.getLiveNodes(nodeID))
            {
                InetAddress host = InetAddress.getByName(liveHost);

                if (changeHostState(host, true))
                {
                    LOG.debug("Host {} marked as UP", host);
                }
            }

            for (String unreachableHost : proxy.getUnreachableNodes(nodeID))
            {
                InetAddress host = InetAddress.getByName(unreachableHost);

                if (changeHostState(host, false))
                {
                    LOG.debug("Host {} marked as DOWN", host);
                }
            }

            myLastRefresh = System.currentTimeMillis();
            return true;
        }
        catch (IOException e)
        {
            THROTTLED_LOGGER.warn("Unable to retrieve host states", e);
        }

        return false;
    }

    private boolean changeHostState(final InetAddress host, final boolean newValue)
    {
        Boolean oldValue = myHostStates.put(host, newValue);

        return oldValue == null || oldValue != newValue;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private long myRefreshIntervalInMs = DEFAULT_REFRESH_INTERVAL_IN_MS;

        public final Builder withJmxProxyFactory(final DistributedJmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public final Builder withRefreshIntervalInMs(final long refreshIntervalInMs)
        {
            myRefreshIntervalInMs = refreshIntervalInMs;
            return this;
        }

        public final HostStatesImpl build()
        {
            if (myJmxProxyFactory == null)
            {
                throw new IllegalArgumentException("JMX Proxy Factory must be set");
            }

            return new HostStatesImpl(this);
        }
    }
}
