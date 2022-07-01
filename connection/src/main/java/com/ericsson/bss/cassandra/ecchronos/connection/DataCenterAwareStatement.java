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

    public DataCenterAwareStatement(BoundStatement statement, String dataCenter)
    {
        myBoundStatement = statement;
        myDataCenter = dataCenter;
    }

    public String getDataCenter()
    {
        return myDataCenter;
    }

    @Override
    public PreparedStatement getPreparedStatement()
    {
        return myBoundStatement.getPreparedStatement();
    }

    @Override
    public List<ByteBuffer> getValues()
    {
        return myBoundStatement.getValues();
    }

    @Override
    public BoundStatement setExecutionProfileName(String newConfigProfileName)
    {
        return myBoundStatement.setExecutionProfileName(newConfigProfileName);
    }

    @Override
    public BoundStatement setExecutionProfile(DriverExecutionProfile newProfile)
    {
        return myBoundStatement.setExecutionProfile(newProfile);
    }

    @Override
    public BoundStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace)
    {
        return myBoundStatement.setRoutingKeyspace(newRoutingKeyspace);
    }

    @Override
    public BoundStatement setNode(Node node)
    {
        return myBoundStatement.setNode(node);
    }

    @Override
    public BoundStatement setRoutingKey(ByteBuffer newRoutingKey)
    {
        return myBoundStatement.setRoutingKey(newRoutingKey);
    }

    @Override
    public BoundStatement setRoutingToken(Token newRoutingToken)
    {
        return myBoundStatement.setRoutingToken(newRoutingToken);
    }

    @Override
    public BoundStatement setCustomPayload(Map<String, ByteBuffer> newCustomPayload)
    {
        return myBoundStatement.setCustomPayload(newCustomPayload);
    }

    @Override
    public BoundStatement setIdempotent(Boolean newIdempotence)
    {
        return myBoundStatement.setIdempotent(newIdempotence);
    }

    @Override
    public BoundStatement setTracing(boolean newTracing)
    {
        return myBoundStatement.setTracing(newTracing);
    }

    @Override
    public long getQueryTimestamp()
    {
        return myBoundStatement.getQueryTimestamp();
    }

    @Override
    public BoundStatement setQueryTimestamp(long newTimestamp)
    {
        return myBoundStatement.setQueryTimestamp(newTimestamp);
    }

    @Override
    public BoundStatement setTimeout(Duration newTimeout)
    {
        return myBoundStatement.setTimeout(newTimeout);
    }

    @Override
    public ByteBuffer getPagingState()
    {
        return myBoundStatement.getPagingState();
    }

    @Override
    public BoundStatement setPagingState(ByteBuffer newPagingState)
    {
        return myBoundStatement.setPagingState(newPagingState);
    }

    @Override
    public int getPageSize()
    {
        return myBoundStatement.getPageSize();
    }

    @Override
    public BoundStatement setPageSize(int newPageSize)
    {
        return myBoundStatement.setPageSize(newPageSize);
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return myBoundStatement.getConsistencyLevel();
    }

    @Override
    public BoundStatement setConsistencyLevel(ConsistencyLevel newConsistencyLevel)
    {
        return myBoundStatement.setConsistencyLevel(newConsistencyLevel);
    }

    @Override
    public ConsistencyLevel getSerialConsistencyLevel()
    {
        return myBoundStatement.getSerialConsistencyLevel();
    }

    @Override
    public BoundStatement setSerialConsistencyLevel(ConsistencyLevel newSerialConsistencyLevel)
    {
        return myBoundStatement.setSerialConsistencyLevel(newSerialConsistencyLevel);
    }

    @Override
    public boolean isTracing()
    {
        return myBoundStatement.isTracing();
    }

    @Override
    public int firstIndexOf(String name)
    {
        return myBoundStatement.firstIndexOf(name);
    }

    @Override
    public int firstIndexOf(CqlIdentifier id)
    {
        return myBoundStatement.firstIndexOf(id);
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i)
    {
        return myBoundStatement.getBytesUnsafe(i);
    }

    @Override
    public BoundStatement setBytesUnsafe(int i, ByteBuffer v)
    {
        return myBoundStatement.setBytesUnsafe(i, v);
    }

    @Override
    public int size()
    {
        return myBoundStatement.size();
    }

    @Override
    public DataType getType(int i)
    {
        return myBoundStatement.getType(i);
    }

    @Override
    public CodecRegistry codecRegistry()
    {
        return myBoundStatement.codecRegistry();
    }

    @Override
    public ProtocolVersion protocolVersion()
    {
        return myBoundStatement.protocolVersion();
    }

    @Override
    public String getExecutionProfileName()
    {
        return myBoundStatement.getExecutionProfileName();
    }

    @Override
    public DriverExecutionProfile getExecutionProfile()
    {
        return myBoundStatement.getExecutionProfile();
    }

    @Override
    public CqlIdentifier getRoutingKeyspace()
    {
        return myBoundStatement.getRoutingKeyspace();
    }

    @Override
    public ByteBuffer getRoutingKey()
    {
        return myBoundStatement.getRoutingKey();
    }

    @Override
    public Token getRoutingToken()
    {
        return myBoundStatement.getRoutingToken();
    }

    @Override
    public Map<String, ByteBuffer> getCustomPayload()
    {
        return myBoundStatement.getCustomPayload();
    }

    @Override
    public Boolean isIdempotent()
    {
        return myBoundStatement.isIdempotent();
    }

    @Override
    public Duration getTimeout()
    {
        return myBoundStatement.getTimeout();
    }

    @Override
    public Node getNode()
    {
        return myBoundStatement.getNode();
    }
}
