/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link HostStates} interface using JMX to retrieve node statuses and then caches the retrieved statuses for some time.
 */
public class HostStatesImpl implements HostStates, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(HostStatesImpl.class);

    private static final long DEFAULT_REFRESH_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(10);

    private final ConcurrentHashMap<InetAddress, Boolean> myHostStates = new ConcurrentHashMap<>();
    private final Object myRefreshLock = new Object();
    private final long myRefreshIntervalInMs;

    private volatile long myLastRefresh = -1;

    private final JmxProxyFactory myJmxProxyFactory;

    private HostStatesImpl(Builder builder)
    {
        myRefreshIntervalInMs = builder.myRefreshIntervalInMs;
        myJmxProxyFactory = builder.myJmxProxyFactory;
    }

    @Override
    public boolean isUp(InetAddress address)
    {
        refreshNodeStatus();

        Boolean status = myHostStates.get(address);
        return status != null && status;
    }

    @Override
    public boolean isUp(com.datastax.oss.driver.api.core.metadata.Node node)
    {
        return isUp(node.getBroadcastAddress().get().getAddress());
    }

    @Override
    public boolean isUp(Node node)
    {
        return isUp(node.getPublicAddress());
    }

    @Override
    public void close()
    {
        myHostStates.clear();
    }

    private void refreshNodeStatus()
    {
        if (shouldRefreshNodeStatus())
        {
            synchronized (myRefreshLock)
            {
                if (shouldRefreshNodeStatus())
                {
                    tryRefreshHostStates();
                }
            }
        }
    }

    private boolean shouldRefreshNodeStatus()
    {
        return myLastRefresh == -1 || myLastRefresh < (System.currentTimeMillis() - myRefreshIntervalInMs);
    }

    private synchronized boolean tryRefreshHostStates()
    {
        if (myJmxProxyFactory == null)
        {
            return false;
        }

        try (JmxProxy proxy = myJmxProxyFactory.connect())
        {
            for (String liveHost : proxy.getLiveNodes())
            {
                InetAddress host = InetAddress.getByName(liveHost);

                if (changeHostState(host, true))
                {
                    LOG.debug("Host {} marked as up", host);
                }
            }

            for (String unreachableHost : proxy.getUnreachableNodes())
            {
                InetAddress host = InetAddress.getByName(unreachableHost);

                if (changeHostState(host, false))
                {
                    LOG.debug("Host {} marked as down", host);
                }
            }

            myLastRefresh = System.currentTimeMillis();
            return true;
        }
        catch (IOException e)
        {
            LOG.warn("Unable to retrieve host states", e);
        }

        return false;
    }

    private boolean changeHostState(InetAddress host, boolean newValue)
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
        private JmxProxyFactory myJmxProxyFactory;
        private long myRefreshIntervalInMs = DEFAULT_REFRESH_INTERVAL_IN_MS;

        public Builder withJmxProxyFactory(JmxProxyFactory jmxProxyFactory)
        {
            myJmxProxyFactory = jmxProxyFactory;
            return this;
        }

        public Builder withRefreshIntervalInMs(long refreshIntervalInMs)
        {
            myRefreshIntervalInMs = refreshIntervalInMs;
            return this;
        }

        public HostStatesImpl build()
        {
            if (myJmxProxyFactory == null)
            {
                throw new IllegalArgumentException("JMX Proxy Factory must be set");
            }

            return new HostStatesImpl(this);
        }
    }
}
