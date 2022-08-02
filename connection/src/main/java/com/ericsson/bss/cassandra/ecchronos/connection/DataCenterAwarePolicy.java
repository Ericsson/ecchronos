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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom load balancing policy based on com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
 * and com.datastax.driver.core.policies.TokenAwarePolicy but extended to allow the local data center
 * to be replaced with a specified data center when creating a new plan.
 */
public class DataCenterAwarePolicy extends DefaultLoadBalancingPolicy
{
    private static final Logger LOG = LoggerFactory.getLogger(DataCenterAwarePolicy.class);

    private final ConcurrentMap<String, CopyOnWriteArrayList<Node>> myPerDcLiveNodes = new ConcurrentHashMap<>();
    private final AtomicInteger myIndex = new AtomicInteger();

    public DataCenterAwarePolicy(DriverContext context, String profileName)
    {
        super(context, profileName);
    }

    @Override
    public void init(Map<UUID, Node> nodes, DistanceReporter distanceReporter)
    {
        super.init(nodes, distanceReporter);
        LOG.info("Using provided data-center name '{}' for DataCenterAwareLoadBalancingPolicy", getLocalDatacenter());

        ArrayList<String> notInLocalDC = new ArrayList<>();

        for (Node node : nodes.values())
        {
            String dc = getDc(node);

            if (!dc.equals(getLocalDatacenter()))
            {
                notInLocalDC.add(String.format("%s (%s)", node, dc));
            }

            CopyOnWriteArrayList<Node> nodeList = myPerDcLiveNodes.get(dc);
            if (nodeList == null)
            {
                myPerDcLiveNodes.put(dc, new CopyOnWriteArrayList<>(Collections.singletonList(node)));
            }
            else
            {
                nodeList.addIfAbsent(node);
            }
        }

        if (!notInLocalDC.isEmpty())
        {
            String nonLocalHosts = Joiner.on(",").join(notInLocalDC);
            LOG.warn("Some contact points don't match local data center. Local DC = {}. Non-conforming contact points: {}", getLocalDatacenter(),
                    nonLocalHosts);
        }

        myIndex.set(new SecureRandom().nextInt(Math.max(nodes.size(), 1)));
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
     * @param request the query for which to build the plan.
     * @return the new query plan.
     */
    @Override
    public Queue<Node> newQueryPlan(Request request, Session session)
    {
        final String dataCenter;

        if (request instanceof DataCenterAwareStatement)
        {
            dataCenter = ((DataCenterAwareStatement) request).getDataCenter();
        }
        else
        {
            return super.newQueryPlan(request, session);
        }

        ByteBuffer partitionKey = request.getRoutingKey();
        CqlIdentifier keyspace = request.getRoutingKeyspace();
        if (partitionKey == null || keyspace == null)
        {
            return getFallbackQueryPlan(dataCenter);
        }
        final Set<Node> replicas = session.getMetadata().getTokenMap()
                .orElseThrow(IllegalStateException::new)
                .getReplicas(keyspace, partitionKey);
        if (replicas.isEmpty())
        {
            return getFallbackQueryPlan(dataCenter);
        }

        return getQueryPlan(dataCenter, replicas);
    }

    private Queue<Node> getQueryPlan(String datacenter, Set<Node> replicas)
    {
        Queue queue = new ConcurrentLinkedQueue();
        for (Node node : replicas)
        {
            if (node.getState().equals(NodeState.UP) && distance(node, datacenter).equals(NodeDistance.LOCAL))
            {
                queue.add(node);
            }
        }
        // Skip if it was already a local replica
        Queue<Node> fallbackQueue = getFallbackQueryPlan(datacenter);
        fallbackQueue.stream().filter(n -> !queue.contains(n)).forEachOrdered(n -> queue.add(n));
        return queue;
    }

    /**
     * Return the {@link NodeDistance} for the provided host according to the selected data center.
     *
     * @param node the node of which to return the distance of.
     * @param dataCenter the selected data center.
     * @return the HostDistance to {@code host}.
     */
    public NodeDistance distance(Node node, String dataCenter)
    {
        String dc = getDc(node);
        if (dc.equals(dataCenter))
        {
            return NodeDistance.LOCAL;
        }

        CopyOnWriteArrayList<Node> dcNodes = myPerDcLiveNodes.get(dc);
        if (dcNodes == null)
        {
            return NodeDistance.IGNORED;
        }

        return dcNodes.contains(node) ? NodeDistance.REMOTE : NodeDistance.IGNORED;
    }

    private Queue<Node> getFallbackQueryPlan(final String dataCenter)
    {
        CopyOnWriteArrayList<Node> localLiveNodes = myPerDcLiveNodes.get(dataCenter);
        final List<Node> nodes = localLiveNodes == null ? Collections.emptyList() : cloneList(localLiveNodes);
        final int startIndex = myIndex.getAndIncrement();
        int index = startIndex;
        int remainingLocal = nodes.size();
        Queue<Node> queue = new ConcurrentLinkedQueue<>();
        while (remainingLocal > 0)
        {
            remainingLocal--;
            int count = index++ % nodes.size();
            if (count < 0)
            {
                count += nodes.size();
            }
            queue.add(nodes.get(count));
        }
        return queue;
    }

    @SuppressWarnings ("unchecked")
    private static CopyOnWriteArrayList<Node> cloneList(CopyOnWriteArrayList<Node> list)
    {
        return (CopyOnWriteArrayList<Node>) list.clone();
    }

    @Override
    public void onUp(Node node)
    {
        super.onUp(node);
        markAsUp(node);
    }

    private void markAsUp(Node node)
    {
        String dc = getDc(node);

        CopyOnWriteArrayList<Node> dcNodes = myPerDcLiveNodes.get(dc);
        if (dcNodes == null)
        {
            CopyOnWriteArrayList<Node> newMap = new CopyOnWriteArrayList<>(Collections.singletonList(node));
            dcNodes = myPerDcLiveNodes.putIfAbsent(dc, newMap);
            // If we've successfully put our new node, we're good, otherwise we've been beaten so continue
            if (dcNodes == null)
            {
                return;
            }
        }
        dcNodes.addIfAbsent(node);
    }

    @Override
    public void onDown(Node node)
    {
        super.onDown(node);
        markAsDown(node);
    }

    private void markAsDown(Node node)
    {
        CopyOnWriteArrayList<Node> dcNodes = myPerDcLiveNodes.get(getDc(node));
        if (dcNodes != null)
        {
            dcNodes.remove(node);
        }
    }

    private String getDc(Node node)
    {
        String dc = node.getDatacenter();
        return dc == null ? getLocalDatacenter() : dc;
    }

    @Override
    public void onAdd(Node node)
    {
        super.onAdd(node);
        markAsUp(node);
    }

    @Override
    public void onRemove(Node node)
    {
        super.onRemove(node);
        markAsDown(node);
    }

    // Only for test
    ConcurrentMap<String, CopyOnWriteArrayList<Node>> getPerDcLiveNodes()
    {
        return myPerDcLiveNodes;
    }
}
