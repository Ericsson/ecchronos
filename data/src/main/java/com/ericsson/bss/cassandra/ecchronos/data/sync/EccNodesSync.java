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
package com.ericsson.bss.cassandra.ecchronos.data.sync;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.ericsson.bss.cassandra.ecchronos.data.enums.NodeStatus;
import com.ericsson.bss.cassandra.ecchronos.data.exceptions.EcChronosException;
import com.google.common.base.Preconditions;

import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

/**
 * CQL Definition for nodes_sync table. CREATE TABLE ecchronos_agent.nodes_sync ( ecchronos_id TEXT, datacenter_name
 * TEXT, node_id UUID, node_endpoint TEXT, node_status TEXT, last_connection TIMESTAMP, next_connection TIMESTAMP,
 * PRIMARY KEY ( ecchronos_id, datacenter_name, node_id ) ) WITH CLUSTERING ORDER BY( datacenter_name DESC, node_id DESC
 * );
 */
public final class EccNodesSync
{
    private static final Logger LOG = LoggerFactory.getLogger(EccNodesSync.class);

    private static final Integer DEFAULT_CONNECTION_DELAY_IN_MINUTES = 30;
    private static final String COLUMN_ECCHRONOS_ID = "ecchronos_id";
    private static final String COLUMN_DC_NAME = "datacenter_name";
    private static final String COLUMN_NODE_ID = "node_id";
    private static final String COLUMN_NODE_ENDPOINT = "node_endpoint";
    private static final String COLUMN_NODE_STATUS = "node_status";
    private static final String COLUMN_LAST_CONNECTION = "last_connection";
    private static final String COLUMN_NEXT_CONNECTION = "next_connection";

    private static final String KEYSPACE_NAME = "ecchronos";
    private static final String TABLE_NAME = "nodes_sync";

    private final CqlSession mySession;
    private final List<Node> myNodesList;
    private final String ecChronosID;

    private final PreparedStatement myCreateStatement;
    private final PreparedStatement myUpdateStatusStatement;
    private final PreparedStatement mySelectStatusStatement;

    private EccNodesSync(final Builder builder) throws UnknownHostException
    {
        mySession = Preconditions.checkNotNull(builder.mySession, "Session cannot be null");
        myNodesList = Preconditions
                .checkNotNull(builder.initialNodesList, "Nodes list cannot be null");
        myCreateStatement = mySession.prepare(QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME)
                .value(COLUMN_ECCHRONOS_ID, bindMarker())
                .value(COLUMN_DC_NAME, bindMarker())
                .value(COLUMN_NODE_ENDPOINT, bindMarker())
                .value(COLUMN_NODE_STATUS, bindMarker())
                .value(COLUMN_LAST_CONNECTION, bindMarker())
                .value(COLUMN_NEXT_CONNECTION, bindMarker())
                .value(COLUMN_NODE_ID, bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
        myUpdateStatusStatement = mySession.prepare(QueryBuilder.update(KEYSPACE_NAME, TABLE_NAME)
                .setColumn(COLUMN_NODE_STATUS, bindMarker())
                .setColumn(COLUMN_LAST_CONNECTION, bindMarker())
                .setColumn(COLUMN_NEXT_CONNECTION, bindMarker())
                .whereColumn(COLUMN_ECCHRONOS_ID).isEqualTo(bindMarker())
                .whereColumn(COLUMN_DC_NAME).isEqualTo(bindMarker())
                .whereColumn(COLUMN_NODE_ID).isEqualTo(bindMarker())
                .build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
        mySelectStatusStatement = mySession.prepare(selectFrom(KEYSPACE_NAME, TABLE_NAME)
                .columns(COLUMN_NODE_ID, COLUMN_NODE_ENDPOINT, COLUMN_DC_NAME, COLUMN_NODE_STATUS)
                .whereColumn(COLUMN_ECCHRONOS_ID).isEqualTo(bindMarker())
                .build());
        ecChronosID = builder.myEcchronosID;
    }

    public ResultSet getResultSet()
    {
        // Bind the parameters
        BoundStatement boundStatement = mySelectStatusStatement.bind(ecChronosID);
        return mySession.execute(boundStatement);
    }

    public void acquireNodes() throws EcChronosException
    {
        if (myNodesList.isEmpty())
        {
            throw new EcChronosException("Cannot Acquire Nodes because there is no nodes to be acquired");
        }
        for (Node node : myNodesList)
        {
            LOG.info(
                    "Preparing to acquire node {} with endpoint {} and Datacenter {}",
                    node.getHostId(),
                    node.getEndPoint(),
                    node.getDatacenter());
            ResultSet tmpResultSet = acquireNode(node);
            if (tmpResultSet.wasApplied())
            {
                LOG.info("Node successfully acquired by instance {}", ecChronosID);
            }
            else
            {
                LOG.error("Unable to acquire node {}", node.getHostId());
            }
        }
    }

    private ResultSet acquireNode(final Node node)
    {
        return insertNodeInfo(
                node.getDatacenter(),
                node.getEndPoint().toString(),
                node.getState().toString(),
                Instant.now(),
                Instant.now().plus(DEFAULT_CONNECTION_DELAY_IN_MINUTES, ChronoUnit.MINUTES),
                node.getHostId());
    }

    @VisibleForTesting
    public ResultSet verifyAcquireNode(final Node node)
    {
        return acquireNode(node);
    }

    private ResultSet insertNodeInfo(
            final String datacenterName,
            final String nodeEndpoint,
            final String nodeStatus,
            final Instant lastConnection,
            final Instant nextConnection,
            final UUID nodeID
    )
    {
        BoundStatement insertNodeSyncInfo = myCreateStatement.bind(ecChronosID,
                datacenterName, nodeEndpoint, nodeStatus, lastConnection, nextConnection, nodeID);
        return execute(insertNodeSyncInfo);
    }

    public ResultSet updateNodeStatus(
            final NodeStatus nodeStatus,
            final String datacenterName,
            final UUID nodeID
    )
    {
        ResultSet tmpResultSet = updateNodeStateStatement(nodeStatus, datacenterName, nodeID);
        if (tmpResultSet.wasApplied())
        {
            LOG.info("Node {} successfully updated", nodeID);
        }
        else
        {
            LOG.error("Unable to update node {}", nodeID);
        }
        return tmpResultSet;
    }

    private ResultSet updateNodeStateStatement(
            final NodeStatus nodeStatus,
            final String datacenterName,
            final UUID nodeID
    )
    {
        BoundStatement updateNodeStatus = myUpdateStatusStatement.bind(
                nodeStatus.toString(),
                Instant.now(),
                Instant.now().plus(DEFAULT_CONNECTION_DELAY_IN_MINUTES, ChronoUnit.MINUTES),
                ecChronosID,
                datacenterName,
                nodeID
        );
        return execute(updateNodeStatus);
    }

    @VisibleForTesting
    public ResultSet verifyInsertNodeInfo(
            final String datacenterName,
            final String nodeEndpoint,
            final String nodeStatus,
            final Instant lastConnection,
            final Instant nextConnection,
            final UUID nodeID
    )
    {
        return insertNodeInfo(
                datacenterName,
                nodeEndpoint,
                nodeStatus,
                lastConnection,
                nextConnection,
                nodeID
        );
    }

    public ResultSet execute(final BoundStatement statement)
    {
        return mySession.execute(statement);
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private CqlSession mySession;
        private List<Node> initialNodesList;
        private String myEcchronosID;

        /**
         * Builds EccNodesSync with session.
         *
         * @param session
         *         Session object
         * @return Builder
         */
        public Builder withSession(final CqlSession session)
        {
            this.mySession = session;
            return this;
        }

        /**
         * Builds EccNodesSync with nodes list.
         *
         * @param nodes
         *         nodes list
         * @return Builder
         */
        public Builder withInitialNodesList(final List<Node> nodes)
        {
            this.initialNodesList = nodes;
            return this;
        }

        /**
         * Builds EccNodesSync with ecchronosID.
         *
         * @param echronosID
         *         ecchronos ID generated by BeanConfigurator.
         * @return Builder
         */
        public Builder withEcchronosID(final String echronosID)
        {
            this.myEcchronosID = echronosID;
            return this;
        }

        /**
         * Builds EccNodesSync.
         *
         * @return Builder
         * @throws UnknownHostException
         */
        public EccNodesSync build() throws UnknownHostException
        {
            return new EccNodesSync(this);
        }
    }
}

