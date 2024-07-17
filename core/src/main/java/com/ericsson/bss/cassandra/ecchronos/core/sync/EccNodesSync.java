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
package com.ericsson.bss.cassandra.ecchronos.core.sync;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.google.common.base.Preconditions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

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

    private static final String ECCHORONS_ID_PRE_STRING = "ecchronos-";
    private static final  String KEYSPACE_NAME = "ecchronos";
    private static final  String TABLE_NAME = "nodes_sync";

    private final CqlSession mySession;
    private final StatementDecorator myStatementDecorator;
    private final List<Node> myNodesList;
    private final String ecChronosID;

    private final PreparedStatement myCreateStatement;

    private EccNodesSync(final Builder builder) throws UnknownHostException
    {
        mySession = Preconditions.checkNotNull(builder.mySession, "Session cannot be null");
        myStatementDecorator = Preconditions
                .checkNotNull(builder.myStatementDecorator, "StatementDecorator cannot be null");
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
        ecChronosID = ECCHORONS_ID_PRE_STRING.concat(InetAddress.getLocalHost().getHostName());
    }

    public List<ResultSet> acquireNodes() throws EcChronosException
    {
        List<ResultSet> resultSets = new ArrayList<>();
        if (!myNodesList.isEmpty())
        {
            for (Node node : myNodesList)
            {
                LOG.info(
                    "Preparing to acquire node {} with endpoint {} and Datacenter {}",
                    node.getHostId(),
                    node.getEndPoint(),
                    node.getDatacenter());
                ResultSet tmpResultSet = acquireNode(node);
                LOG.info("Node successfully acquired by instance {}", ecChronosID);
                resultSets.add(tmpResultSet);
            }
        }
        else
        {
            throw new EcChronosException("Cannot Acquire Nodes because there is no nodes to be acquired");
        }
        return resultSets;
    }

    private ResultSet acquireNode(final Node node)
    {
        return insertNodeInfo(
            ecChronosID,
            node.getDatacenter(),
            node.getEndPoint().toString(),
            node.getState().toString(),
            Instant.now(),
            Instant.now().plus(DEFAULT_CONNECTION_DELAY_IN_MINUTES, ChronoUnit.MINUTES),
            node.getHostId());
    }

    @VisibleForTesting
    public ResultSet verifyAquireNode(final Node node)
    {
        return acquireNode(node);
    }

    private ResultSet insertNodeInfo(
        final String ecchronosID,
        final String datacenterName,
        final String nodeEndpoint,
        final String nodeStatus,
        final Instant lastConnection,
        final Instant nextConnection,
        final UUID nodeID)
    {
        BoundStatement insertNodeSyncInfo = myCreateStatement.bind(ecchronosID,
                datacenterName, nodeEndpoint, nodeStatus, lastConnection, nextConnection, nodeID);
        return execute(insertNodeSyncInfo);
    }

    @VisibleForTesting
    ResultSet verifyInsertNodeInfo(
        final String ecchronosID,
        final String datacenterName,
        final String nodeEndpoint,
        final String nodeStatus,
        final Instant lastConnection,
        final Instant nextConnection,
        final UUID nodeID)
    {
        return insertNodeInfo(
            ecchronosID,
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
        return mySession.execute(myStatementDecorator.apply(statement));
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private CqlSession mySession;
        private StatementDecorator myStatementDecorator;
        private List<Node> initialNodesList;

        /**
         * Builds EccNodesSync with session.
         *
         * @param session Session object
         * @return Builder
         */
        public Builder withSession(final CqlSession session)
        {
            this.mySession = session;
            return this;
        }

        /**
         * Builds EccNodesSync with Statement decorator.
         *
         * @param statementDecorator Statement decorator
         * @return Builder
         */
        public Builder withStatementDecorator(final StatementDecorator statementDecorator)
        {
            this.myStatementDecorator = statementDecorator;
            return this;
        }

        /**
         * Builds EccNodesSync with nodes list.
         *
         * @param nodes nodes list
         * @return Builder
         */
        public Builder withInitialNodesList(final List<Node> nodes)
        {
            this.initialNodesList = nodes;
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
