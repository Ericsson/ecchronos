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
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.providers.DistributedNativeConnectionProviderImpl;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedNativeBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(DistributedNativeBuilder.class);

    private ConnectionType myType = ConnectionType.datacenterAware;
    private List<InetSocketAddress> myInitialContactPoints = new ArrayList<>();
    private String myLocalDatacenter = "datacenter1";
    private String ecchronosKeyspaceName = "ecchronos";

    private List<String> myDatacenterAware = new ArrayList<>();
    private List<Map<String, String>> myRackAware = new ArrayList<>();
    private List<InetSocketAddress> myHostAware = new ArrayList<>();

    private boolean myIsMetricsEnabled = true;
    private AuthProvider myAuthProvider = null;
    private SslEngineFactory mySslEngineFactory = null;
    private SchemaChangeListener mySchemaChangeListener = null;
    private final Set<NodeStateListener> myNodeStateListeners = new HashSet<>();

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
     *         the type of the agent as a {@link ConnectionType}.
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

    public final DistributedNativeBuilder withEcchronosKeyspaceName(final String keySpace)
    {
        ecchronosKeyspaceName = keySpace;
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
        myNodeStateListeners.add(nodeStateListener);
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
        DistributedNativeConnectionProviderImpl connectionProvider;
        CqlSession session = null;
        try
        {
            LOG.info("Creating Session With Initial Contact Points");
            session = CqlSessionFactory.create(myInitialContactPoints, myLocalDatacenter, myAuthProvider,
                    mySslEngineFactory, mySchemaChangeListener, myNodeStateListeners, myIsMetricsEnabled);
            LOG.info("Requesting Nodes List");
            Map<UUID, Node> nodesList = createNodesMap(session);
            LOG.info("Nodes list was created with success");
            Optional<KeyspaceMetadata> keyspaceMetadata = session
                    .getMetadata()
                    .getKeyspace(ecchronosKeyspaceName);
            if (keyspaceMetadata.isEmpty())
            {
                throw new IllegalStateException("ecchronos Keyspace is not setup yet");
            }
            Map<String, String> replication = keyspaceMetadata.get().getReplication();

            connectionProvider = new DistributedNativeConnectionProviderImpl(session, nodesList, this, myType);
            connectionProvider.isKeyspaceReplicationFactorOK(replication, myLocalDatacenter);
        }
        catch (RuntimeException e)
        {
            if (session != null)
            {
                session.close();
            }
            throw e;
        }
        return connectionProvider;
    }

    /**
     * Creates a map of nodes based on the connection type, reads the node list from the database.
     * @param session the connection information to the database
     * @return map of nodes
     */
    public Map<UUID, Node> createNodesMap(final CqlSession session)
    {
        NodeFilter filter = createNodeFilter();
        List<Node> nodes = filter.resolve(session);
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
        return createNodeFilter().isValid(node);
    }

    private NodeFilter createNodeFilter()
    {
        return switch (myType)
        {
            case datacenterAware -> new DatacenterNodeFilter(myDatacenterAware);
            case rackAware -> new RackNodeFilter(myRackAware);
            case hostAware -> new HostNodeFilter(myHostAware);
        };
    }
}
