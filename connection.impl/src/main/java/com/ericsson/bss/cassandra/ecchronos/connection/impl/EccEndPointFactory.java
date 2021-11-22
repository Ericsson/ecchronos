/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.connection.impl;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.EndPointFactory;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Row;

/**
 * EndPointFactory that creates wrapped EndPoints for remote hosts and always returns the same EndPoint for the local
 * host.
 *
 * The wrapped EndPoints determine equality based on the host_id value of the row to allow for changing IP addresses.
 */
public class EccEndPointFactory implements EndPointFactory, Host.StateListener
{
    private static final Logger LOG = LoggerFactory.getLogger(EccEndPointFactory.class);

    private final ConcurrentHashMap<UUID, HostIdEndPoint> hostIdEndPointMap = new ConcurrentHashMap<>();

    private final EndPoint localEndPoint;
    private final UUID localHost;
    private final EndPointFactory delegateEndpointFactory;

    public EccEndPointFactory(EndPoint localEndPoint, UUID localHost, EndPointFactory delegateEndpointFactory)
    {
        this.localEndPoint = localEndPoint;
        this.localHost = localHost;
        this.delegateEndpointFactory = delegateEndpointFactory;
    }

    @Override
    public void init(Cluster cluster)
    {
        delegateEndpointFactory.init(cluster);
    }

    @Override
    public EndPoint create(Row peersRow)
    {
        UUID hostId = peersRow.getUUID("host_id");
        if (hostId == null)
        {
            return null;
        }

        if (localHost.equals(hostId))
        {
            return localEndPoint;
        }

        EndPoint endPoint = delegateEndpointFactory.create(peersRow);
        if (endPoint != null)
        {
            HostIdEndPoint existing = hostIdEndPointMap.get(hostId);

            if (existing != null)
            {
                EndPoint existingWrapped = existing.getWrapped();
                LOG.info("Updating host EndPoint for '{}' from '{}' -> '{}'", hostId, existingWrapped, endPoint);
                existing.setEndPoint(endPoint);
                return existing;
            }

            LOG.info("Adding host '{}' to known hosts with EndPoint '{}'", hostId, endPoint);

            // Retain the wrapping EndPoint object but replace the wrapped EndPoint as the driver
            // reuses the old Host object if the EndPoints are equal.
            return hostIdEndPointMap.compute(hostId, (uuid, old) -> {
                if (old != null)
                {
                    old.setEndPoint(endPoint);
                    return old;
                }

                return new HostIdEndPoint(endPoint, hostId);
            });
        }

        return null;
    }

    @Override
    public void onAdd(Host host)
    {
        // Do nothing
    }

    @Override
    public void onUp(Host host)
    {
        // Do nothing
    }

    @Override
    public void onDown(Host host)
    {
        // Do nothing
    }

    @Override
    public void onRemove(Host host)
    {
        UUID hostId = host.getHostId();
        if (hostId != null)
        {
            HostIdEndPoint removedEndPoint = hostIdEndPointMap.remove(hostId);
            if (removedEndPoint != null)
            {
                LOG.info("Removed host '{}' with EndPoint '{}' from known hosts", hostId, removedEndPoint.getWrapped());
            }
            else
            {
                LOG.debug("Host '{}' was already removed from known hosts, notified more than once", hostId);
            }
        }
        else
        {
            LOG.warn("No host ID found for host {} while trying to remove cached EndPoint", host);
        }
    }

    @Override
    public void onRegister(Cluster cluster)
    {
        // Do nothing
    }

    @Override
    public void onUnregister(Cluster cluster)
    {
        // Do nothing
    }

    static class HostIdEndPoint implements EndPoint
    {
        private final UUID hostId;
        private volatile EndPoint wrapped;

        HostIdEndPoint(EndPoint wrapped, UUID hostId)
        {
            this.wrapped = wrapped;
            this.hostId = hostId;
        }

        void setEndPoint(EndPoint wrapped)
        {
            this.wrapped = wrapped;
        }

        EndPoint getWrapped()
        {
            return wrapped;
        }

        @Override
        public InetSocketAddress resolve()
        {
            return wrapped.resolve();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            HostIdEndPoint that = (HostIdEndPoint) o;
            return hostId.equals(that.hostId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hostId);
        }

        @Override
        public String toString()
        {
            return String.format("%s(%s)", hostId, wrapped);
        }
    }
}
