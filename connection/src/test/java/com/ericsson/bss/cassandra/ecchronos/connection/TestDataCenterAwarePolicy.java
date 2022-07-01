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

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.session.Session;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    private DriverContext myDriverContextMock;

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
    }

    @Test
    @Ignore
    public void testBuilderWithLocalDcAsNull()
    {
        //TODO
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() ->
                        new DataCenterAwarePolicy(myDriverContextMock, ""));
    }

    @Test
    public void testDistanceHost()
    {
        //TODO
        /*DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);

        NodeDistance distance1 = policy.distance(myNodeDC1Mock);
        NodeDistance distance2 = policy.distance(myNodeDC2Mock);
        NodeDistance distance3 = policy.distance(myNodeDC3Mock);
        NodeDistance distance4 = policy.distance(myNodeNoDCMock);
        NodeDistance distance5 = policy.distance(myNodeNotDC3Mock);

        assertThat(distance1).isEqualTo(HostDistance.LOCAL);
        assertThat(distance2).isEqualTo(HostDistance.REMOTE);
        assertThat(distance3).isEqualTo(HostDistance.REMOTE);
        assertThat(distance4).isEqualTo(HostDistance.IGNORED);
        assertThat(distance5).isEqualTo(HostDistance.IGNORED);*/
    }

    @Test
    public void testDistanceHostString()
    {
        //TODO
        /*DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(mySessionMock, myNodes);

        HostDistance distance1 = policy.distance(myNodeDC1Mock, myRemoteDc);
        HostDistance distance2 = policy.distance(myNodeDC2Mock, myRemoteDc);
        HostDistance distance3 = policy.distance(myNodeDC3Mock, myRemoteDc);
        HostDistance distance4 = policy.distance(myNodeNoDCMock, myRemoteDc);
        HostDistance distance5 = policy.distance(myNodeNotDC3Mock, myRemoteDc);

        assertThat(distance1).isEqualTo(HostDistance.REMOTE);
        assertThat(distance2).isEqualTo(HostDistance.LOCAL);
        assertThat(distance3).isEqualTo(HostDistance.REMOTE);
        assertThat(distance4).isEqualTo(HostDistance.IGNORED);
        assertThat(distance5).isEqualTo(HostDistance.IGNORED);*/
    }

    @Test
    @Ignore
    public void testNewQueryPlanWithNotPartitionAwareStatement()
    {
        //TODO
        SimpleStatement simpleStatement = SimpleStatement.newInstance("SELECT *");

        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");

        policy.init(myNodes, myDistanceReporterMock);

        policy.newQueryPlan(simpleStatement, mySessionMock);

        verify(policy, times(1)).newQueryPlan(simpleStatement, mySessionMock);
    }

    @Test
    public void testNewQueryPlanWithPartitionAwareStatementLocalDc()
    {
        //TODO
        /*Map<UUID, Node> nodes = new HashMap<>();
        nodes.put(UUID.randomUUID(), myNodeDC1Mock);
        when(myTokenMapMock.getReplicas(any(CqlIdentifier.class), any(ByteBuffer.class))).thenReturn(nodes);

        SimpleStatement simpleStatement = SimpleStatement.newInstance("SELECT *");
        simpleStatement.setKeyspace("foo");
        simpleStatement.setRoutingKey(ByteBuffer.wrap("foo".getBytes()));
        DataCenterAwareStatement partitionAwareStatement = new DataCenterAwareStatement(simpleStatement, myLocalDc);

        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");

        policy.init(myNodes, myDistanceReporterMock);

        Iterator<Host> iterator = policy.newQueryPlan(null, partitionAwareStatement);

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(myNodeDC1Mock);
        assertThat(iterator.hasNext()).isFalse();*/
    }

    @Test
    public void testNewQueryPlanWithPartitionAwareStatementRemoteDc()
    {
        //TODO
        /*Map<UUID, Node> nodes = new HashMap<>();
        nodes.put(UUID.randomUUID(), myNodeDC1Mock);
        when(myTokenMapMock.getReplicas(any(CqlIdentifier.class), any(ByteBuffer.class))).thenReturn(nodes);

        SimpleStatement simpleStatement = SimpleStatement.newInstance("SELECT *");
        simpleStatement.setKeyspace("foo");
        simpleStatement.setRoutingKey(ByteBuffer.wrap("foo".getBytes()));
        DataCenterAwareStatement partitionAwareStatement = new DataCenterAwareStatement(simpleStatement, myRemoteDc);

        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");

        policy.init(myNodes, myDistanceReporterMock);

        Queue<Node> queue = policy.newQueryPlan(partitionAwareStatement, mySessionMock);

        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.poll()).isEqualTo(myNodeDC2Mock);
        assertThat(queue.isEmpty()).isTrue();*/
    }

    @Test
    public void testInit()
    {
        //TODO
        /*
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");

        policy.init(myNodes, myDistanceReporterMock);

        verify(myChildPolicy, times(1)).init(mySessionMock, myNodes);*/
    }

    @Test
    public void testOnUp()
    {
        //TODO
        /*DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);
        policy.onUp(myNodeDC1Mock);
        verify(myChildPolicy, times(1)).onUp(myNodeDC1Mock);*/
    }

    @Test
    public void testOnDown()
    {
        //TODO
        /*
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);
        policy.onDown(myNodeDC1Mock);
        verify(myChildPolicy, times(1)).onDown(myNodeDC1Mock);*/
    }

    @Test
    public void testOnAdd()
    {
        //TODO
        /*
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);
        policy.onAdd(myNodeDC1Mock);
        verify(myChildPolicy, times(1)).onAdd(myNodeDC1Mock);*/
    }

    @Test
    public void testOnRemove()
    {
        //TODO
        /*
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.init(myNodes, myDistanceReporterMock);
        policy.onRemove(myNodeDC1Mock);
        verify(myChildPolicy, times(1)).onRemove(myNodeDC1Mock);*/
    }

    @Test
    public void testClose()
    {
        //TODO
        /*
        DataCenterAwarePolicy policy = new DataCenterAwarePolicy(myDriverContextMock, "");
        policy.close();
        verify(myChildPolicy, times(1)).close();*/
    }
}
