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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedNativeConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import com.google.common.collect.ImmutableList;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedNativeBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(DistributedNativeBuilder.class);

    private static final List<String> SCHEMA_REFRESHED_KEYSPACES = ImmutableList.of("/.*/", "!system",
            "!system_distributed", "!system_schema", "!system_traces", "!system_views", "!system_virtual_schema");

    private static final List<String> SESSION_METRICS = Arrays.asList(DefaultSessionMetric.BYTES_RECEIVED.getPath(),
            DefaultSessionMetric.BYTES_SENT.getPath(), DefaultSessionMetric.CONNECTED_NODES.getPath(),
            DefaultSessionMetric.CQL_REQUESTS.getPath(), DefaultSessionMetric.CQL_CLIENT_TIMEOUTS.getPath(),
            DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath(), DefaultSessionMetric.THROTTLING_DELAY.getPath(),
            DefaultSessionMetric.THROTTLING_QUEUE_SIZE.getPath(), DefaultSessionMetric.THROTTLING_ERRORS.getPath());

    private ConnectionType myType = ConnectionType.datacenterAware;
    private List<InetSocketAddress> myInitialContactPoints = new ArrayList<>();
    private String myLocalDatacenter = "datacenter1";

    private List<String> myDatacenterAware = new ArrayList<>();
    private List<Map<String, String>> myRackAware = new ArrayList<>();
    private List<InetSocketAddress> myHostAware = new ArrayList<>();

    private boolean myIsMetricsEnabled = true;
    private AuthProvider myAuthProvider = null;
    private SslEngineFactory mySslEngineFactory = null;
    private SchemaChangeListener mySchemaChangeListener = null;
    private NodeStateListener myNodeStateListener = null;

    /**
     * Sets the initial contact points for the distributed native connection.
     *
     * @param initialContactPoints
     *         the list of initial contact points as {@link InetSocketAddress} instances.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withInitialContactPoints(final List<InetSocketAddress> initialContactPoints)
    {
        myInitialContactPoints = initialContactPoints;
        return this;
    }

    /**
     * Sets the type of the agent for the distributed native connection.
     *
     * @param type
     *         the type of the agent as a {@link String}.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withAgentType(final ConnectionType type)
    {
        myType = type;
        return this;
    }

    /**
     * Sets the local datacenter for the distributed native connection.
     *
     * @param localDatacenter
     *         the name of the local datacenter.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withLocalDatacenter(final String localDatacenter)
    {
        myLocalDatacenter = localDatacenter;
        return this;
    }

    /**
     * Sets the datacenter awareness for the distributed native connection.
     *
     * @param datacentersInfo
     *         a list of datacenter information as {@link String}.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withDatacenterAware(final List<String> datacentersInfo)
    {
        myDatacenterAware = datacentersInfo;
        return this;
    }

    /**
     * Sets the rack awareness for the distributed native connection.
     *
     * @param racksInfo
     *         a list of rack information as {@link Map} of strings.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withRackAware(final List<Map<String, String>> racksInfo)
    {
        myRackAware = racksInfo;
        return this;
    }

    /**
     * Sets the host awareness for the distributed native connection.
     *
     * @param hostsInfo
     *         a list of host information as {@link InetSocketAddress} instances.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withHostAware(final List<InetSocketAddress> hostsInfo)
    {
        myHostAware = hostsInfo;
        return this;
    }

    /**
     * Sets the authentication provider for the distributed native connection.
     *
     * @param authProvider
     *         the {@link AuthProvider} to use for authentication.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withAuthProvider(final AuthProvider authProvider)
    {
        myAuthProvider = authProvider;
        return this;
    }

    /**
     * Sets the SSL engine factory for the distributed native connection.
     *
     * @param sslEngineFactory
     *         the {@link SslEngineFactory} to use for SSL connections.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withSslEngineFactory(final SslEngineFactory sslEngineFactory)
    {
        this.mySslEngineFactory = sslEngineFactory;
        return this;
    }

    /**
     * Sets the schema change listener for the distributed native connection.
     *
     * @param schemaChangeListener
     *         the {@link SchemaChangeListener} to handle schema changes.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withSchemaChangeListener(final SchemaChangeListener schemaChangeListener)
    {
        mySchemaChangeListener = schemaChangeListener;
        return this;
    }

    /**
     * Sets the node state listener for the distributed native connection.
     *
     * @param nodeStateListener
     *         the {@link NodeStateListener} to handle node state changes.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withNodeStateListener(final NodeStateListener nodeStateListener)
    {
        myNodeStateListener = nodeStateListener;
        return this;
    }

    /**
     * Enables or disables metrics for the distributed native connection.
     *
     * @param enabled
     *         true to enable metrics, false to disable.
     * @return the current instance of {@link DistributedNativeBuilder}.
     */
    public final DistributedNativeBuilder withMetricsEnabled(final boolean enabled)
    {
        myIsMetricsEnabled = enabled;
        return this;
    }

    /**
     * Builds and returns a {@link DistributedNativeConnectionProviderImpl} instance.
     *
     * @return a new instance of {@link DistributedNativeConnectionProviderImpl}.
     */
    public final DistributedNativeConnectionProviderImpl build()
    {
        LOG.info("Creating Session With Initial Contact Points");
        CqlSession session = createSession(this);
        LOG.info("Requesting Nodes List");
        Map<UUID, Node> nodesList = createNodesMap(session);
        LOG.info("Nodes list was created with success");
        return new DistributedNativeConnectionProviderImpl(session, nodesList, this, myType);
    }

    /**
     * Creates a map of nodes based on the connection type, reads the node list from the database.
     * @param session the connection information to the database
     * @return map of nodes
     */
    public Map<UUID, Node> createNodesMap(final CqlSession session)
    {
        return switch (myType)
        {
            case datacenterAware -> generateNodesMap(resolveDatacenterNodes(session, myDatacenterAware));
            case rackAware -> generateNodesMap(resolveRackNodes(session, myRackAware));
            case hostAware -> generateNodesMap(resolveHostAware(session, myHostAware));
        };
    }

    private Map<UUID, Node> generateNodesMap(final List<Node> nodes)
    {
        Map<UUID, Node> nodesMap = new HashMap<>();
        nodes.forEach(node -> nodesMap.put(node.getHostId(), node));
        return nodesMap;
    }

    /**
     * Checks the node is on the list of specified dc's/racks/nodes.
     * @param node the node to validate.
     * @return true if node is valid.
     */
    public Boolean confirmNodeValid(final Node node)
    {
        return switch (myType)
        {
            case datacenterAware -> confirmDatacenterNodeValid(node, myDatacenterAware);
            case rackAware -> confirmRackNodeValid(node, myRackAware);
            case hostAware -> confirmHostNodeValid(node, myHostAware);
        };
    }

    private Boolean confirmDatacenterNodeValid(final Node node, final List<String> datacenterNames)
    {
        return (datacenterNames.contains(node.getDatacenter()));
    }

    private Boolean confirmRackNodeValid(final Node node, final List<Map<String, String>> rackInfo)
    {
        Set<Map<String, String>> racksInfoSet = new HashSet<>(rackInfo);
        Map<String, String> tmpRackInfo = new HashMap<>();
        tmpRackInfo.put("datacenterName", node.getDatacenter());
        tmpRackInfo.put("rackName", node.getRack());
        return (racksInfoSet.contains(tmpRackInfo));
    }

    private Boolean confirmHostNodeValid(final Node node, final List<InetSocketAddress> hostsInfo)
    {
        Set<InetSocketAddress> hostsInfoSet = new HashSet<>(hostsInfo);

        InetSocketAddress tmpAddress = (InetSocketAddress) node.getEndPoint().resolve();
        return (hostsInfoSet.contains(tmpAddress));
     }

    private CqlSession createSession(final DistributedNativeBuilder builder)
    {
        CqlSessionBuilder sessionBuilder = fromBuilder(builder);

        DriverConfigLoader driverConfigLoader = loaderBuilder(builder).build();
        LOG.debug("Driver configuration: {}", driverConfigLoader.getInitialConfig().getDefaultProfile().entrySet());
        sessionBuilder.withConfigLoader(driverConfigLoader);
        return sessionBuilder.build();
    }

    private List<Node> resolveDatacenterNodes(final CqlSession session, final List<String> datacenterNames)
    {
        Set<String> datacenterNameSet = new HashSet<>(datacenterNames);
        List<Node> nodesList = new ArrayList<>();
        Collection<Node> nodes = session.getMetadata().getNodes().values();

        for (Node node : nodes)
        {
            if (datacenterNameSet.contains(node.getDatacenter()))
            {
                nodesList.add(node);
            }
        }
        return nodesList;
    }

    private List<Node> resolveRackNodes(final CqlSession session, final List<Map<String, String>> rackInfo)
    {
        Set<Map<String, String>> racksInfoSet = new HashSet<>(rackInfo);
        List<Node> nodesList = new ArrayList<>();
        Collection<Node> nodes = session.getMetadata().getNodes().values();

        for (Node node : nodes)
        {
            Map<String, String> tmpRackInfo = new HashMap<>();
            tmpRackInfo.put("datacenterName", node.getDatacenter());
            tmpRackInfo.put("rackName", node.getRack());
            if (racksInfoSet.contains(tmpRackInfo))
            {
                nodesList.add(node);
            }
        }
        return nodesList;
    }

    private List<Node> resolveHostAware(final CqlSession session, final List<InetSocketAddress> hostsInfo)
    {
        Set<InetSocketAddress> hostsInfoSet = new HashSet<>(hostsInfo);
        List<Node> nodesList = new ArrayList<>();
        Collection<Node> nodes = session.getMetadata().getNodes().values();
        for (Node node : nodes)
        {
            InetSocketAddress tmpAddress = (InetSocketAddress) node.getEndPoint().resolve();
            if (hostsInfoSet.contains(tmpAddress))
            {
                nodesList.add(node);
            }
        }
        return nodesList;
    }

    private static ProgrammaticDriverConfigLoaderBuilder loaderBuilder(
            final DistributedNativeBuilder builder
    )
    {
        ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder()
                .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
                        SCHEMA_REFRESHED_KEYSPACES);
        if (builder.myIsMetricsEnabled)
        {
            loaderBuilder.withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, SESSION_METRICS);
            loaderBuilder.withString(DefaultDriverOption.METRICS_FACTORY_CLASS, "MicrometerMetricsFactory");
            loaderBuilder.withString(DefaultDriverOption.METRICS_ID_GENERATOR_CLASS, "TaggingMetricIdGenerator");
        }
        return loaderBuilder;
    }

    private static CqlSessionBuilder fromBuilder(final DistributedNativeBuilder builder)
    {
        return CqlSession.builder()
                .addContactPoints(resolveIfNeeded(builder.myInitialContactPoints))
                .withLocalDatacenter(builder.myLocalDatacenter)
                .withAuthProvider(builder.myAuthProvider)
                .withSslEngineFactory(builder.mySslEngineFactory)
                .withSchemaChangeListener(builder.mySchemaChangeListener)
                .withNodeStateListener(builder.myNodeStateListener);
    }

    private static Collection<InetSocketAddress> resolveIfNeeded(final List<InetSocketAddress> initialContactPoints)
    {
        List<InetSocketAddress> resolvedContactPoints = new ArrayList<>();
        for (InetSocketAddress address : initialContactPoints)
        {
            if (address.isUnresolved())
            {
                resolvedContactPoints.add(new InetSocketAddress(address.getHostString(), address.getPort()));
            }
            else
            {
                resolvedContactPoints.add(address);
            }
        }
        return resolvedContactPoints;
    }

    /**
     * Resolves nodes in the specified datacenters for testing purposes. This method delegates to
     * {@link #resolveDatacenterNodes(CqlSession, List)}.
     *
     * @param session
     *         the {@link CqlSession} used to connect to the cluster.
     * @param datacenterNames
     *         the list of datacenter names to resolve nodes for.
     * @return a list of {@link Node} instances representing the resolved nodes.
     */
    @VisibleForTesting
    public final List<Node> testResolveDatacenterNodes(final CqlSession session, final List<String> datacenterNames)
    {
        return resolveDatacenterNodes(session, datacenterNames);
    }

    /**
     * Resolves nodes in the specified racks for testing purposes. This method delegates to
     * {@link #resolveRackNodes(CqlSession, List)}.
     *
     * @param session
     *         the {@link CqlSession} used to connect to the cluster.
     * @param rackInfo
     *         a list of maps representing rack information.
     * @return a list of {@link Node} instances representing the resolved nodes.
     */
    @VisibleForTesting
    public final List<Node> testResolveRackNodes(final CqlSession session, final List<Map<String, String>> rackInfo)
    {
        return resolveRackNodes(session, rackInfo);
    }

    /**
     * Resolves nodes based on host awareness for testing purposes. This method delegates to
     * {@link #resolveHostAware(CqlSession, List)}.
     *
     * @param session
     *         the {@link CqlSession} used to connect to the cluster.
     * @param hostsInfo
     *         a list of {@link InetSocketAddress} representing host information.
     * @return a list of {@link Node} instances representing the resolved nodes.
     */
    @VisibleForTesting
    public final List<Node> testResolveHostAware(final CqlSession session, final List<InetSocketAddress> hostsInfo)
    {
        return resolveHostAware(session, hostsInfo);
    }
}
