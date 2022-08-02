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
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestDataCenterAwarePolicy
{
    private final String myLocalDc = "DC1";
    private final String myRemoteDc = "DC2";

    @Mock
    private Session mySessionMock;

    @Mock
    private Metadata myMetadataMock;

    @Mock
    private TokenMap myTokenMapMock;

    @Mock
    private InternalDriverContext myDriverContextMock;

    @Mock
    private DriverConfig myDriverConfigMock;

    @Mock
    private DriverExecutionProfile myDriverExecutionProfileMock;

    @Mock
    private ConsistencyLevelRegistry myConsistencyLevelRegistryMock;

    @Mock
    private MetadataManager myMetadataManagerMock;

    @Mock
    private LoadBalancingPolicy.DistanceReporter myDistanceReporterMock;

    @Mock
    private Node myNodeDC1Mock;

    @Mock
    private Node myNodeDC2Mock;

    @Mock
    private Node myNodeDC3Mock;

    @Mock
    private Node myNodeNoDCMock;

    @Mock
    private Node myNodeNotDC3Mock;

    private Map<UUID, Node> myNodes = new HashMap<>();

    @Before
    public void setup()
    {
        when(mySessionMock.getMetadata()).thenReturn(myMetadataMock);
        when(myMetadataMock.getTokenMap()).thenReturn(Optional.of(myTokenMapMock));

        when(myNodeDC1Mock.getDatacenter()).thenReturn("DC1");
        when(myNodeDC1Mock.getState()).thenReturn(NodeState.UP);
        when(myNodeDC2Mock.getDatacenter()).thenReturn("DC2");
        when(myNodeDC2Mock.getState()).thenReturn(NodeState.UP);
        when(myNodeDC3Mock.getDatacenter()).thenReturn("DC3");
        when(myNodeDC3Mock.getState()).thenReturn(NodeState.UP);
        when(myNodeNoDCMock.getDatacenter()).thenReturn("no DC");
        when(myNodeNoDCMock.getState()).thenReturn(NodeState.UP);
        when(myNodeNotDC3Mock.getDatacenter()).thenReturn("DC3");
        when(myNodeNotDC3Mock.getState()).thenReturn(NodeState.UP);

        myNodes.put(UUID.randomUUID(), myNodeDC1Mock);
        myNodes.put(UUID.randomUUID(), myNodeDC2Mock);
        myNodes.put(UUID.randomUUID(), myNodeDC3Mock);
        when(myDriverContextMock.getConfig()).thenReturn(myDriverConfigMock);
        when(myDriverContextMock.getLocalDatacenter(any())).thenReturn(myLocalDc);
        when(myDriverContextMock.getMetadataManager()).thenReturn(myMetadataManagerMock);
        when(myMetadataManagerMock.getMetadata()).thenReturn(myMetadataMock);
        when(myDriverConfigMock.getProfile(any(String.class))).thenReturn(myDriverExecutionProfileMock);
        when(myDriverExecutionProfileMock.getName()).thenReturn("unittest");
        when(myDriverExecutionProfileMock.getInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC)).thenReturn(999);
        when(myDriverExecutionProfileMock.getBoolean(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS)).thenReturn(false);
        when(myDriverExecutionProfileMock.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("LOCAL_QUORUM");
        when(myDriverContextMock.getConsistencyLevelRegistry()).thenReturn(myConsistencyLevelRegistryMock);
        when(myConsistencyLevelRegistryMock.nameToLevel(any(String.class))).thenReturn(ConsistencyLevel.LOCAL_QUORUM);
    }

    @Test
    public void testDistanceHost()
    {
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);

        NodeDistance distance1 = policy.distance(myNodeDC1Mock, myLocalDc);
        NodeDistance distance2 = policy.distance(myNodeDC2Mock, myLocalDc);
        NodeDistance distance3 = policy.distance(myNodeDC3Mock, myLocalDc);
        NodeDistance distance4 = policy.distance(myNodeNoDCMock, myLocalDc);
        NodeDistance distance5 = policy.distance(myNodeNotDC3Mock, myLocalDc);

        assertThat(distance1).isEqualTo(NodeDistance.LOCAL);
        assertThat(distance2).isEqualTo(NodeDistance.REMOTE);
        assertThat(distance3).isEqualTo(NodeDistance.REMOTE);
        assertThat(distance4).isEqualTo(NodeDistance.IGNORED);
        assertThat(distance5).isEqualTo(NodeDistance.IGNORED);
    }

    @Test
    public void testNewQueryPlanWithNotPartitionAwareStatement()
    {
        SimpleStatement simpleStatement = SimpleStatement.newInstance("SELECT *");

        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");

        policy.init(myNodes, myDistanceReporterMock);

        Set<Node> nodes = new HashSet<>();
        nodes.add(myNodeDC1Mock);
        when(myTokenMapMock.getReplicas(any(CqlIdentifier.class), any(ByteBuffer.class))).thenReturn(nodes);
        Queue<Node> queue = policy.newQueryPlan(simpleStatement, mySessionMock);

        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.poll()).isEqualTo(myNodeDC1Mock);
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    public void testNewQueryPlanWithPartitionAwareStatementLocalDc()
    {
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(boundStatement.getRoutingKeyspace()).thenReturn(CqlIdentifier.fromInternal("foo"));
        when(boundStatement.getRoutingKey()).thenReturn(ByteBuffer.wrap("foo".getBytes()));
        DataCenterAwareStatement partitionAwareStatement = new DataCenterAwareStatement(boundStatement, myLocalDc);

        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");

        policy.init(myNodes, myDistanceReporterMock);

        Set<Node> nodes = new HashSet<>();
        nodes.add(myNodeDC1Mock);
        when(myTokenMapMock.getReplicas(any(CqlIdentifier.class), any(ByteBuffer.class))).thenReturn(nodes);

        Queue<Node> queue = policy.newQueryPlan(partitionAwareStatement, mySessionMock);

        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.poll()).isEqualTo(myNodeDC1Mock);
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    public void testNewQueryPlanWithPartitionAwareStatementRemoteDc()
    {
        BoundStatement boundStatement = mock(BoundStatement.class);
        when(boundStatement.getRoutingKeyspace()).thenReturn(CqlIdentifier.fromInternal("foo"));
        when(boundStatement.getRoutingKey()).thenReturn(ByteBuffer.wrap("foo".getBytes()));
        DataCenterAwareStatement partitionAwareStatement = new DataCenterAwareStatement(boundStatement, myRemoteDc);

        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");

        policy.init(myNodes, myDistanceReporterMock);

        Set<Node> nodes = new HashSet<>();
        nodes.add(myNodeDC1Mock);
        when(myTokenMapMock.getReplicas(any(CqlIdentifier.class), any(ByteBuffer.class))).thenReturn(nodes);

        Queue<Node> queue = policy.newQueryPlan(partitionAwareStatement, mySessionMock);

        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.poll()).isEqualTo(myNodeDC2Mock);
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    public void testOnUp()
    {
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);

        CopyOnWriteArrayList<Node> nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(1);

        policy.onUp(myNodeNotDC3Mock);

        nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.contains(myNodeNotDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(2);
    }

    @Test
    public void testOnUpTwice()
    {
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);

        CopyOnWriteArrayList<Node> nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(1);

        policy.onUp(myNodeNotDC3Mock);
        policy.onUp(myNodeNotDC3Mock);

        nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.contains(myNodeNotDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(2);
    }

    @Test
    public void testOnDown()
    {
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);
        CopyOnWriteArrayList<Node> nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(1);

        policy.onDown(myNodeDC3Mock);

        nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC).isEmpty();
    }

    @Test
    public void testOnDownTwice()
    {
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);
        CopyOnWriteArrayList<Node> nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(1);

        policy.onDown(myNodeDC3Mock);
        policy.onDown(myNodeDC3Mock);

        nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC).isEmpty();
    }

    @Test
    public void testOnAdd()
    {
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);

        CopyOnWriteArrayList<Node> nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(1);

        policy.onAdd(myNodeNotDC3Mock);

        nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.contains(myNodeNotDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(2);
    }

    @Test
    public void testOnRemove()
    {
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);
        CopyOnWriteArrayList<Node> nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC.contains(myNodeDC3Mock));
        assertThat(nodesInDC.size()).isEqualTo(1);

        policy.onRemove(myNodeDC3Mock);

        nodesInDC = policy.getPerDcLiveNodes().get("DC3");
        assertThat(nodesInDC).isEmpty();
    }
}
