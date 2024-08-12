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
package com.ericsson.bss.cassandra.ecchronos.connection;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DataCenterAwareStatement implements BoundStatement
{
    private final String myDataCenter;
    private final BoundStatement myBoundStatement;

    public DataCenterAwareStatement(final BoundStatement statement, final String dataCenter)
    {
        myBoundStatement = statement;
        myDataCenter = dataCenter;
    }

    public final String getDataCenter()
    {
        return myDataCenter;
    }

    @Override
    public final PreparedStatement getPreparedStatement()
    {
        return myBoundStatement.getPreparedStatement();
    }

    @Override
    public final List<ByteBuffer> getValues()
    {
        return myBoundStatement.getValues();
    }

    @Override
    public final BoundStatement setExecutionProfileName(final String newConfigProfileName)
    {
        return myBoundStatement.setExecutionProfileName(newConfigProfileName);
    }

    @Override
    public final BoundStatement setExecutionProfile(final DriverExecutionProfile newProfile)
    {
        return myBoundStatement.setExecutionProfile(newProfile);
    }

    @Override
    public final BoundStatement setRoutingKeyspace(final CqlIdentifier newRoutingKeyspace)
    {
        return myBoundStatement.setRoutingKeyspace(newRoutingKeyspace);
    }

    @Override
    public final BoundStatement setNode(final Node node)
    {
        return myBoundStatement.setNode(node);
    }

    @Override
    public final BoundStatement setRoutingKey(final ByteBuffer newRoutingKey)
    {
        return myBoundStatement.setRoutingKey(newRoutingKey);
    }

    @Override
    public final BoundStatement setRoutingToken(final Token newRoutingToken)
    {
        return myBoundStatement.setRoutingToken(newRoutingToken);
    }

    @Override
    public final BoundStatement setCustomPayload(final Map<String, ByteBuffer> newCustomPayload)
    {
        return myBoundStatement.setCustomPayload(newCustomPayload);
    }

    @Override
    public final BoundStatement setIdempotent(final Boolean newIdempotence)
    {
        return myBoundStatement.setIdempotent(newIdempotence);
    }

    @Override
    public final BoundStatement setTracing(final boolean newTracing)
    {
        return myBoundStatement.setTracing(newTracing);
    }

    @Override
    public final long getQueryTimestamp()
    {
        return myBoundStatement.getQueryTimestamp();
    }

    @Override
    public final BoundStatement setQueryTimestamp(final long newTimestamp)
    {
        return myBoundStatement.setQueryTimestamp(newTimestamp);
    }

    @Override
    public final BoundStatement setTimeout(final Duration newTimeout)
    {
        return myBoundStatement.setTimeout(newTimeout);
    }

    @Override
    public final ByteBuffer getPagingState()
    {
        return myBoundStatement.getPagingState();
    }

    @Override
    public final BoundStatement setPagingState(final ByteBuffer newPagingState)
    {
        return myBoundStatement.setPagingState(newPagingState);
    }

    @Override
    public final int getPageSize()
    {
        return myBoundStatement.getPageSize();
    }

    @Override
    public final BoundStatement setPageSize(final int newPageSize)
    {
        return myBoundStatement.setPageSize(newPageSize);
    }

    @Override
    public final ConsistencyLevel getConsistencyLevel()
    {
        return myBoundStatement.getConsistencyLevel();
    }

    @Override
    public final BoundStatement setConsistencyLevel(final ConsistencyLevel newConsistencyLevel)
    {
        return myBoundStatement.setConsistencyLevel(newConsistencyLevel);
    }

    @Override
    public final ConsistencyLevel getSerialConsistencyLevel()
    {
        return myBoundStatement.getSerialConsistencyLevel();
    }

    @Override
    public final BoundStatement setSerialConsistencyLevel(final ConsistencyLevel newSerialConsistencyLevel)
    {
        return myBoundStatement.setSerialConsistencyLevel(newSerialConsistencyLevel);
    }

    @Override
    public final boolean isTracing()
    {
        return myBoundStatement.isTracing();
    }

    @Override
    public final int firstIndexOf(final String name)
    {
        return myBoundStatement.firstIndexOf(name);
    }

    @Override
    public final int firstIndexOf(final CqlIdentifier id)
    {
        return myBoundStatement.firstIndexOf(id);
    }

    @Override
    public final ByteBuffer getBytesUnsafe(final int i)
    {
        return myBoundStatement.getBytesUnsafe(i);
    }

    @Override
    public final BoundStatement setBytesUnsafe(final int i, final ByteBuffer v)
    {
        return myBoundStatement.setBytesUnsafe(i, v);
    }

    @Override
    public final int size()
    {
        return myBoundStatement.size();
    }

    @Override
    public final DataType getType(final int i)
    {
        return myBoundStatement.getType(i);
    }

    @Override
    public final CodecRegistry codecRegistry()
    {
        return myBoundStatement.codecRegistry();
    }

    @Override
    public final ProtocolVersion protocolVersion()
    {
        return myBoundStatement.protocolVersion();
    }

    @Override
    public final String getExecutionProfileName()
    {
        return myBoundStatement.getExecutionProfileName();
    }

    @Override
    public final DriverExecutionProfile getExecutionProfile()
    {
        return myBoundStatement.getExecutionProfile();
    }

    @Override
    public final CqlIdentifier getRoutingKeyspace()
    {
        return myBoundStatement.getRoutingKeyspace();
    }

    @Override
    public final ByteBuffer getRoutingKey()
    {
        return myBoundStatement.getRoutingKey();
    }

    @Override
    public final Token getRoutingToken()
    {
        return myBoundStatement.getRoutingToken();
    }

    @Override
    public final Map<String, ByteBuffer> getCustomPayload()
    {
        return myBoundStatement.getCustomPayload();
    }

    @Override
    public final Boolean isIdempotent()
    {
        return myBoundStatement.isIdempotent();
    }

    @Override
    public final Duration getTimeout()
    {
        return myBoundStatement.getTimeout();
    }

    @Override
    public final Node getNode()
    {
        return myBoundStatement.getNode();
    }
}

