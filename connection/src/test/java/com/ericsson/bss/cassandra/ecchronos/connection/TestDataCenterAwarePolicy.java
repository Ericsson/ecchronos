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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;

@RunWith (MockitoJUnitRunner.Silent.class)
public class TestDataCenterAwarePolicy
{
    private final String myLocalDc = "DC1";
    private final String myRemoteDc = "DC2";

    @Mock
    private LoadBalancingPolicy myChildPolicy;

    @Mock
    private Cluster myClusterMock;

    @Mock
    private Metadata myMetadataMock;

    @Mock
    private Configuration myConfigMock;

    @Mock
    private ProtocolOptions myProtocolOptionsMock;

    @Mock
    private Host myHostDC1Mock;

    @Mock
    private Host myHostDC2Mock;

    @Mock
    private Host myHostDC3Mock;

    @Mock
    private Host myHostNoDCMock;

    @Mock
    private Host myHostNotDC3Mock;

    private List<Host> myHostList;

    @Before
    public void setup()
    {
        when(myClusterMock.getMetadata()).thenReturn(myMetadataMock);
        when(myClusterMock.getConfiguration()).thenReturn(myConfigMock);

        when(myConfigMock.getProtocolOptions()).thenReturn(myProtocolOptionsMock);
        when(myConfigMock.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);

        when(myProtocolOptionsMock.getProtocolVersion()).thenReturn(ProtocolVersion.V4);

        when(myHostDC1Mock.getDatacenter()).thenReturn("DC1");
        when(myHostDC1Mock.isUp()).thenReturn(true);
        when(myHostDC2Mock.getDatacenter()).thenReturn("DC2");
        when(myHostDC2Mock.isUp()).thenReturn(true);
        when(myHostDC3Mock.getDatacenter()).thenReturn("DC3");
        when(myHostDC3Mock.isUp()).thenReturn(true);
        when(myHostNoDCMock.getDatacenter()).thenReturn("no DC");
        when(myHostNoDCMock.isUp()).thenReturn(true);
        when(myHostNotDC3Mock.getDatacenter()).thenReturn("DC3");
        when(myHostNotDC3Mock.isUp()).thenReturn(true);

        myHostList = Arrays.asList(myHostDC1Mock, myHostDC2Mock, myHostDC3Mock);
    }

    @Test
    public void testBuilderWithValidInput()
    {
        DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();
    }

    @Test
    public void testBuilderWithChildPolicyAsNull()
    {
        LoadBalancingPolicy childPolicy = null;

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> DataCenterAwarePolicy.builder()
                        .withChildPolicy(childPolicy)
                        .withLocalDc(myLocalDc)
                        .build());
    }

    @Test
    public void testBuilderWithLocalDcAsNull()
    {
        String localDc = null;

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() ->
                        DataCenterAwarePolicy.builder()
                                .withChildPolicy(myChildPolicy)
                                .withLocalDc(localDc).build());
    }

    @Test
    public void testGetChildPolicy()
    {
        LoadBalancingPolicy childPolicy = Policies.defaultLoadBalancingPolicy();

        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(childPolicy).withLocalDc(myLocalDc).build();

        assertThat(policy.getChildPolicy()).isEqualTo(childPolicy);
    }

    @Test
    public void testDistanceHost()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        HostDistance distance1 = policy.distance(myHostDC1Mock);
        HostDistance distance2 = policy.distance(myHostDC2Mock);
        HostDistance distance3 = policy.distance(myHostDC3Mock);
        HostDistance distance4 = policy.distance(myHostNoDCMock);
        HostDistance distance5 = policy.distance(myHostNotDC3Mock);

        assertThat(distance1).isEqualTo(HostDistance.LOCAL);
        assertThat(distance2).isEqualTo(HostDistance.REMOTE);
        assertThat(distance3).isEqualTo(HostDistance.REMOTE);
        assertThat(distance4).isEqualTo(HostDistance.IGNORED);
        assertThat(distance5).isEqualTo(HostDistance.IGNORED);
    }

    @Test
    public void testDistanceHostString()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        HostDistance distance1 = policy.distance(myHostDC1Mock, myRemoteDc);
        HostDistance distance2 = policy.distance(myHostDC2Mock, myRemoteDc);
        HostDistance distance3 = policy.distance(myHostDC3Mock, myRemoteDc);
        HostDistance distance4 = policy.distance(myHostNoDCMock, myRemoteDc);
        HostDistance distance5 = policy.distance(myHostNotDC3Mock, myRemoteDc);

        assertThat(distance1).isEqualTo(HostDistance.REMOTE);
        assertThat(distance2).isEqualTo(HostDistance.LOCAL);
        assertThat(distance3).isEqualTo(HostDistance.REMOTE);
        assertThat(distance4).isEqualTo(HostDistance.IGNORED);
        assertThat(distance5).isEqualTo(HostDistance.IGNORED);
    }

    @Test
    public void testNewQueryPlanWithNotPartitionAwareStatement()
    {
        SimpleStatement simpleStatement = new SimpleStatement("SELECT *");

        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        policy.newQueryPlan(null, simpleStatement);

        verify(myChildPolicy, times(1)).newQueryPlan(null, simpleStatement);
    }

    @Test
    public void testNewQueryPlanWithPartitionAwareStatementLocalDc()
    {
        Set<Host> hostSet = new HashSet<>();
        hostSet.add(myHostDC1Mock);
        when(myMetadataMock.getReplicas(anyString(), any(ByteBuffer.class))).thenReturn(hostSet);

        SimpleStatement simpleStatement = new SimpleStatement("SELECT *");
        simpleStatement.setKeyspace("foo");
        simpleStatement.setRoutingKey(ByteBuffer.wrap("foo".getBytes()));
        DataCenterAwareStatement partitionAwareStatement = new DataCenterAwareStatement(simpleStatement, myLocalDc);

        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        Iterator<Host> iterator = policy.newQueryPlan(null, partitionAwareStatement);

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(myHostDC1Mock);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testNewQueryPlanWithPartitionAwareStatementRemoteDc()
    {
        Set<Host> hostSet = new HashSet<>();
        hostSet.add(myHostDC1Mock);
        when(myMetadataMock.getReplicas(anyString(), any(ByteBuffer.class))).thenReturn(hostSet);

        SimpleStatement simpleStatement = new SimpleStatement("SELECT *");
        simpleStatement.setKeyspace("foo");
        simpleStatement.setRoutingKey(ByteBuffer.wrap("foo".getBytes()));
        DataCenterAwareStatement partitionAwareStatement = new DataCenterAwareStatement(simpleStatement, myRemoteDc);

        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        Iterator<Host> iterator = policy.newQueryPlan(null, partitionAwareStatement);

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo(myHostDC2Mock);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    public void testInit()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        verify(myChildPolicy, times(1)).init(myClusterMock, myHostList);
    }

    @Test
    public void testOnUp()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        policy.onUp(myHostDC1Mock);

        verify(myChildPolicy, times(1)).onUp(myHostDC1Mock);
    }

    @Test
    public void testOnDown()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        policy.onDown(myHostDC1Mock);

        verify(myChildPolicy, times(1)).onDown(myHostDC1Mock);
    }

    @Test
    public void testOnAdd()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        policy.onAdd(myHostDC1Mock);

        verify(myChildPolicy, times(1)).onAdd(myHostDC1Mock);
    }

    @Test
    public void testOnRemove()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.init(myClusterMock, myHostList);

        policy.onRemove(myHostDC1Mock);

        verify(myChildPolicy, times(1)).onRemove(myHostDC1Mock);
    }

    @Test
    public void testClose()
    {
        DataCenterAwarePolicy policy = DataCenterAwarePolicy.builder().withChildPolicy(myChildPolicy).withLocalDc(myLocalDc).build();

        policy.close();

        verify(myChildPolicy, times(1)).close();
    }
}
