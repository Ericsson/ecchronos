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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.ericsson.bss.cassandra.ecchronos.core.impl.logging.ThrottlingLogger;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.locks.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.locks.HostStates;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;
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
    private final CqlSession myCqlSession;

    private volatile long myLastRefresh = -1;

    private final DistributedJmxProxyFactory myJmxProxyFactory;

    private HostStatesImpl(final Builder builder)
    {
        myRefreshIntervalInMs = builder.myRefreshIntervalInMs;
        myJmxProxyFactory = builder.myJmxProxyFactory;
        myCqlSession = builder.myCqlSession;
    }

    @Override
    public boolean isUp(final InetAddress address)
    {
        refreshNodeStatus(address);

        Boolean status = myHostStates.get(address);
        return status != null && status;
    }

    @Override
    public boolean isUp(final Node node)
    {
        return isUp(node.getBroadcastAddress().get().getAddress());
    }

    @Override
    public boolean isUp(final DriverNode node)
    {
        return isUp(node.getPublicAddress());
    }

    @Override
    public void close()
    {
        myHostStates.clear();
    }

    private void refreshNodeStatus(final InetAddress address)
    {
        if (shouldRefreshNodeStatus())
        {
            synchronized (myRefreshLock)
            {
                if (shouldRefreshNodeStatus() && !tryRefreshHostStates(address))
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

    private synchronized boolean tryRefreshHostStates(final InetAddress address)
    {
        if (myJmxProxyFactory == null)
        {
            return false;
        }

        UUID hostId = getHostIdForAddress(address);
        try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
        {
            for (String liveHost : proxy.getLiveNodes(hostId))
            {
                InetAddress host = InetAddress.getByName(liveHost);

                if (changeHostState(host, true))
                {
                    LOG.debug("Host {} marked as UP", host);
                }
            }

            for (String unreachableHost : proxy.getUnreachableNodes(hostId))
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

    private UUID getHostIdForAddress(final InetAddress address)
    {
        Metadata metadata = myCqlSession.getMetadata();
        Optional<Node> nodeOptional = metadata.getNodes()
                .values()
                .stream()
                .filter(node -> node.getBroadcastAddress().isPresent()
                        && node.getBroadcastAddress().get().getAddress().equals(address))
                .findFirst();

        return nodeOptional.map(Node::getHostId).orElse(null);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private DistributedJmxProxyFactory myJmxProxyFactory;
        private long myRefreshIntervalInMs = DEFAULT_REFRESH_INTERVAL_IN_MS;
        private CqlSession myCqlSession;

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

        public final Builder withCqlSession(final CqlSession session)
        {
            myCqlSession = session;
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
