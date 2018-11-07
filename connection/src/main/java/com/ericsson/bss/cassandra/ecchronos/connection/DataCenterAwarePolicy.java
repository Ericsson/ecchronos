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
package com.ericsson.bss.cassandra.ecchronos.connection;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.ChainableLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

/**
 * A custom load balancing policy based on com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
 * and com.datastax.driver.core.policies.TokenAwarePolicy but extended to allow the local data center
 * to be replaced with a specified data center when creating a new plan.
 */
public class DataCenterAwarePolicy implements ChainableLoadBalancingPolicy
{
    private static final Logger LOG = LoggerFactory.getLogger(DataCenterAwarePolicy.class);

    private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> myPerDcLiveHosts = new ConcurrentHashMap<>();
    private final AtomicInteger myIndex = new AtomicInteger();
    private final String myLocalDc;
    private final LoadBalancingPolicy myChildPolicy;

    private volatile Metadata myClusterMetadata;
    private volatile ProtocolVersion myProtocolVersion;
    private volatile CodecRegistry myCodecRegistry;

    /**
     * Returns a builder to create a new instance.
     *
     * @return the builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    DataCenterAwarePolicy(String localDc, LoadBalancingPolicy childPolicy)
    {
        if (Strings.isNullOrEmpty(localDc))
        {
            throw new IllegalArgumentException("Null or empty data center specified for data-center-aware policy");
        }
        if (childPolicy == null)
        {
            throw new IllegalArgumentException("Null or empty child policy specified for data-center-aware policy");
        }
        myLocalDc = localDc;
        myChildPolicy = childPolicy;
    }

    @Override
    public LoadBalancingPolicy getChildPolicy()
    {
        return myChildPolicy;
    }

    @Override
    public void init(Cluster cluster, Collection<Host> hosts)
    {
        LOG.info("Using provided data-center name '{}' for DataCenterAwareLoadBalancingPolicy", myLocalDc);

        myClusterMetadata = cluster.getMetadata();
        myProtocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        myCodecRegistry = cluster.getConfiguration().getCodecRegistry();

        ArrayList<String> notInLocalDC = new ArrayList<>();

        for (Host host : hosts)
        {
            String dc = getDc(host);

            if (!dc.equals(myLocalDc))
            {
                notInLocalDC.add(String.format("%s (%s)", host.toString(), dc));
            }

            CopyOnWriteArrayList<Host> hostList = myPerDcLiveHosts.get(dc);
            if (hostList == null)
            {
                myPerDcLiveHosts.put(dc, new CopyOnWriteArrayList<>(Collections.singletonList(host)));
            }
            else
            {
                hostList.addIfAbsent(host);
            }
        }

        if (!notInLocalDC.isEmpty())
        {
            String nonLocalHosts = Joiner.on(",").join(notInLocalDC);
            LOG.warn("Some contact points don't match local data center. Local DC = {}. Non-conforming contact points: {}", myLocalDc,
                    nonLocalHosts);
        }

        myIndex.set(new SecureRandom().nextInt(Math.max(hosts.size(), 1)));

        myChildPolicy.init(cluster, hosts);
    }

    private String getDc(Host host)
    {
        String dc = host.getDatacenter();
        return dc == null ? myLocalDc : dc;
    }

    @SuppressWarnings ("unchecked")
    private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list)
    {
        return (CopyOnWriteArrayList<Host>) list.clone();
    }

    /**
     * Return the {@link HostDistance} for the provided host.
     * <p>
     * This policy consider nodes in the local datacenter as {@code LOCAL}.
     * For each remote datacenter, it considers hosts as {@code REMOTE}.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    @Override
    public HostDistance distance(Host host)
    {
        return distance(host, myLocalDc);
    }

    /**
     * Return the {@link HostDistance} for the provided host according to the selected data center.
     *
     * @param host the host of which to return the distance of.
     * @return the HostDistance to {@code host}.
     */
    public HostDistance distance(Host host, String dataCenter)
    {
        String dc = getDc(host);
        if (dc.equals(dataCenter))
        {
            return HostDistance.LOCAL;
        }

        CopyOnWriteArrayList<Host> dcHosts = myPerDcLiveHosts.get(dc);
        if (dcHosts == null)
        {
            return HostDistance.IGNORED;
        }

        return dcHosts.contains(host) ? HostDistance.REMOTE : HostDistance.IGNORED;
    }

    /**
     * Returns the hosts to use for a new query.
     * <p>
     * The returned plan will first return replicas (whose {@code HostDistance}
     * is {@code LOCAL}) for the query if it can determine
     * them (i.e. mainly if {@code statement.getRoutingKey()} is not {@code null}).
     * Following what it will return hosts whose {@code HostDistance}
     * is {@code LOCAL} according to a Round-robin algorithm.
     * If no specific data center is asked for the child policy is used.
     *
     * @param statement the query for which to build the plan.
     * @return the new query plan.
     */
    @Override
    public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement)
    {
        final String dataCenter;

        if (statement instanceof DataCenterAwareStatement)
        {
            dataCenter = ((DataCenterAwareStatement) statement).getDataCenter();
        }
        else
        {
            return myChildPolicy.newQueryPlan(loggedKeyspace, statement);
        }

        ByteBuffer partitionKey = statement.getRoutingKey(myProtocolVersion, myCodecRegistry);
        String keyspace = statement.getKeyspace();
        if (keyspace == null)
        {
            keyspace = loggedKeyspace;
        }

        if (partitionKey == null || keyspace == null)
        {
            return newFallbackQueryPlan(dataCenter);
        }

        final Set<Host> replicas = myClusterMetadata.getReplicas(Metadata.quote(keyspace), partitionKey);
        if (replicas.isEmpty())
        {
            return newFallbackQueryPlan(dataCenter);
        }

        return new QueryPlanIterator(dataCenter, replicas);
    }

    private class QueryPlanIterator extends AbstractIterator<Host>
    {
        private Iterator<Host> myChildIterator;
        private final String myDataCenter;
        private final Set<Host> myReplicas;
        private final Iterator<Host> myIterator;

        public QueryPlanIterator(String dataCenter, Set<Host> replicas)
        {
            myDataCenter = dataCenter;
            myReplicas = Collections.unmodifiableSet(replicas);
            myIterator = myReplicas.iterator();
        }

        @Override
        protected Host computeNext()
        {
            while (myIterator.hasNext())
            {
                Host host = myIterator.next();
                if (host.isUp() && distance(host, myDataCenter) == HostDistance.LOCAL)
                {
                    return host;
                }
            }

            if (myChildIterator == null)
            {
                myChildIterator = newFallbackQueryPlan(myDataCenter);
            }

            while (myChildIterator.hasNext())
            {
                Host host = myChildIterator.next();
                // Skip it if it was already a local replica
                if (!myReplicas.contains(host) || distance(host, myDataCenter) != HostDistance.LOCAL)
                {
                    return host;
                }
            }
            return endOfData();
        }
    }

    /**
     * Returns the hosts to use for a new query.
     * <p/>
     * The returned plan will only try known host in the specified datacenter.
     * The order of the nodes in the returned query plan will follow a
     * Round-robin algorithm.
     * @param dataCenter     the data center for which to build the plan.
     * @return a new query plan, i.e. an iterator indicating which host to
     * try first for querying, which one to use as failover, etc...
     */
    private Iterator<Host> newFallbackQueryPlan(final String dataCenter)
    {
        CopyOnWriteArrayList<Host> localLiveHosts = myPerDcLiveHosts.get(dataCenter);
        final List<Host> hosts = localLiveHosts == null ? Collections.<Host>emptyList() : cloneList(localLiveHosts);
        final int startIndex = myIndex.getAndIncrement();

        return new AbstractIterator<Host>()
        {

            private int index = startIndex;
            private int remainingLocal = hosts.size();

            @Override
            protected Host computeNext()
            {
                while (true)
                {
                    if (remainingLocal > 0)
                    {
                        remainingLocal--;
                        int count = index++ % hosts.size();
                        if (count < 0)
                        {
                            count += hosts.size();
                        }
                        return hosts.get(count);
                    }

                    return endOfData();
                }
            }
        };
    }

    @Override
    public void onUp(Host host)
    {
        myChildPolicy.onUp(host);
        markAsUp(host);
    }

    private void markAsUp(Host host)
    {
        String dc = getDc(host);

        CopyOnWriteArrayList<Host> dcHosts = myPerDcLiveHosts.get(dc);
        if (dcHosts == null)
        {
            CopyOnWriteArrayList<Host> newMap = new CopyOnWriteArrayList<>(Collections.singletonList(host));
            dcHosts = myPerDcLiveHosts.putIfAbsent(dc, newMap);
            // If we've successfully put our new host, we're good, otherwise we've been beaten so continue
            if (dcHosts == null)
            {
                return;
            }
        }
        dcHosts.addIfAbsent(host);
    }

    @Override
    public void onDown(Host host)
    {
        myChildPolicy.onDown(host);
        markAsDown(host);
    }

    private void markAsDown(Host host)
    {
        CopyOnWriteArrayList<Host> dcHosts = myPerDcLiveHosts.get(getDc(host));
        if (dcHosts != null)
        {
            dcHosts.remove(host);
        }
    }

    @Override
    public void onAdd(Host host)
    {
        myChildPolicy.onAdd(host);
        markAsUp(host);
    }

    @Override
    public void onRemove(Host host)
    {
        myChildPolicy.onRemove(host);
        markAsDown(host);
    }

    @Override
    public void close()
    {
        myChildPolicy.close();
    }

    /**
     * Helper class to build the policy.
     */
    public static class Builder
    {
        private LoadBalancingPolicy myChildPolicy;
        private String myLocalDc;

        public Builder withChildPolicy(LoadBalancingPolicy childPolicy)
        {
            Preconditions.checkArgument(childPolicy != null, "childPolicy must not be null");
            myChildPolicy = childPolicy;
            return this;
        }

        /**
         * Sets the name of the datacenter that will be considered "local" by the policy.
         * <p>
         * This must be the name as known by Cassandra (in other words, the name in that appears in
         * {@code system.peers}, or in the output of admin tools like nodetool).
         *
         * @param localDc the name of the datacenter. It should not be {@code null}.
         * @return this builder.
         */
        public Builder withLocalDc(String localDc)
        {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(localDc), "localDc name can't be null or empty.");
            myLocalDc = localDc;
            return this;
        }

        /**
         * Builds the policy configured by this builder.
         *
         * @return the policy.
         */
        public DataCenterAwarePolicy build()
        {
            return new DataCenterAwarePolicy(myLocalDc, myChildPolicy);
        }
    }
}
