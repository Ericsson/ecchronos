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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.TokenUtil;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestReplicationStateImpl
{
    @Mock
    private NodeResolver mockNodeResolver;

    @Mock
    private CqlSession mockSession;

    @Mock
    private Metadata mockMetadata;

    @Mock
    private TokenMap mockTokenMap;

    @Mock
    private Node mockReplica1;

    @Mock
    private Node mockReplica2;

    @Mock
    private Node mockReplica3;

    @Mock
    private Node mockReplica4;

    @Mock
    private DriverNode mockNode1;

    @Mock
    private DriverNode mockNode2;

    @Mock
    private DriverNode mockNode3;

    @Mock
    private DriverNode mockNode4;

    @Before
    public void setup() throws Exception
    {
        InetAddress address1 = InetAddress.getByName("127.0.0.1");
        InetSocketAddress address11 = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9042);
        InetAddress address2 = InetAddress.getByName("127.0.0.2");
        InetSocketAddress address22 = new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 9042);
        InetAddress address3 = InetAddress.getByName("127.0.0.3");
        InetSocketAddress address33 = new InetSocketAddress(InetAddress.getByName("127.0.0.3"), 9042);
        InetAddress address4 = InetAddress.getByName("127.0.0.4");
        InetSocketAddress address44 = new InetSocketAddress(InetAddress.getByName("127.0.0.4"), 9042);

        when(mockReplica1.getBroadcastAddress()).thenReturn(Optional.of(address11));
        when(mockReplica2.getBroadcastAddress()).thenReturn(Optional.of(address22));
        when(mockReplica3.getBroadcastAddress()).thenReturn(Optional.of(address33));
        when(mockReplica4.getBroadcastAddress()).thenReturn(Optional.of(address44));

        when(mockNodeResolver.fromIp(eq(address1))).thenReturn(Optional.of(mockNode1));
        when(mockNodeResolver.fromIp(eq(address2))).thenReturn(Optional.of(mockNode2));
        when(mockNodeResolver.fromIp(eq(address3))).thenReturn(Optional.of(mockNode3));
        when(mockNodeResolver.fromIp(eq(address4))).thenReturn(Optional.of(mockNode4));

        when(mockMetadata.getTokenMap()).thenReturn(Optional.of(mockTokenMap));
        when(mockSession.getMetadata()).thenReturn(mockMetadata);
    }

    @Test
    public void testGetTokenRangeToReplicaSingleToken() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 2);

        doReturn(Sets.newHashSet(tokenRange)).when(mockTokenMap).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Map<LongTokenRange, Set<DriverNode>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(
                tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);

        assertThat(replicationState.getNodes(tableReference, range1)).isSameAs(tokenRangeToReplicas.get(range1));
    }

    @Test
    public void testGetTokenRangeToReplica() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange1 = TokenUtil.getRange(1, 2);
        TokenRange tokenRange2 = TokenUtil.getRange(2, 3);

        doReturn(Sets.newHashSet(tokenRange1, tokenRange2)).when(mockTokenMap)
                .getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2)).when(mockTokenMap).getReplicas(eq("ks"), eq(tokenRange1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica3)).when(mockTokenMap).getReplicas(eq("ks"), eq(tokenRange2));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Map<LongTokenRange, Set<DriverNode>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(
                tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1, range2);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockNode1, mockNode2);
        assertThat(tokenRangeToReplicas.get(range2)).containsExactlyInAnyOrder(mockNode1, mockNode3);

        assertThat(replicationState.getNodes(tableReference, range1)).isSameAs(tokenRangeToReplicas.get(range1));
        assertThat(replicationState.getNodes(tableReference, range2)).isSameAs(tokenRangeToReplicas.get(range2));
    }

    @Test
    public void testGetReplicas() throws Exception
    {
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange1 = TokenUtil.getRange(1, 2);
        TokenRange tokenRange2 = TokenUtil.getRange(2, 3);

        doReturn(Sets.newHashSet(tokenRange1, tokenRange2)).when(mockTokenMap)
                .getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2)).when(mockTokenMap).getReplicas(eq("ks"), eq(tokenRange1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica3)).when(mockTokenMap).getReplicas(eq("ks"), eq(tokenRange2));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Set<DriverNode> replicas = replicationState.getReplicas(tableReference);

        assertThat(replicas).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);
    }

    @Test
    public void testGetTokenRangeToReplicaSetReuse() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange1 = TokenUtil.getRange(1, 2);
        TokenRange tokenRange2 = TokenUtil.getRange(2, 3);

        doReturn(Sets.newHashSet(tokenRange1, tokenRange2)).when(mockTokenMap)
                .getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2)).when(mockTokenMap).getReplicas(eq("ks"), eq(tokenRange1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2)).when(mockTokenMap).getReplicas(eq("ks"), eq(tokenRange2));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Map<LongTokenRange, Set<DriverNode>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(
                tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1, range2);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockNode1, mockNode2);
        assertThat(tokenRangeToReplicas.get(range1)).isSameAs(tokenRangeToReplicas.get(range2));

        assertThat(replicationState.getNodes(tableReference, range1)).isSameAs(tokenRangeToReplicas.get(range1));
        assertThat(replicationState.getNodes(tableReference, range2)).isSameAs(tokenRangeToReplicas.get(range2));
    }

    @Test
    public void testGetTokenRangeToReplicaMapReuse() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 2);

        doReturn(Sets.newHashSet(tokenRange)).when(mockTokenMap).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Map<LongTokenRange, Set<DriverNode>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(
                tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);

        assertThat(replicationState.getTokenRangeToReplicas(tableReference)).isSameAs(tokenRangeToReplicas);

        assertThat(replicationState.getNodes(tableReference, range1)).isSameAs(tokenRangeToReplicas.get(range1));
    }

    @Test
    public void testGetNodesForSubRange() throws Exception
    {
        LongTokenRange subRange = new LongTokenRange(2, 3);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 5);

        doReturn(Sets.newHashSet(tokenRange)).when(mockTokenMap).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Set<DriverNode> nodes = replicationState.getNodes(tableReference, subRange);

        assertThat(nodes).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);
    }

    @Test
    public void testGetNodesForNonExistingSubRange() throws Exception
    {
        LongTokenRange subRange = new LongTokenRange(6, 7);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 5);

        doReturn(Sets.newHashSet(tokenRange)).when(mockTokenMap).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        assertThat(replicationState.getNodes(tableReference, subRange)).isNull();
    }

    @Test
    public void testGetNodesForIntersectingSubRange() throws Exception
    {
        LongTokenRange subRange = new LongTokenRange(4, 7);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange existingRange = TokenUtil.getRange(1, 5);
        TokenRange existingRange2 = TokenUtil.getRange(5, 9);

        doReturn(Sets.newHashSet(existingRange, existingRange2)).when(mockTokenMap)
                .getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(existingRange));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(existingRange2));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        assertThat(replicationState.getNodes(tableReference, subRange)).isNull();
    }

    @Test
    public void testGetNodesClusterWideForSubRange() throws Exception
    {
        LongTokenRange subRange = new LongTokenRange(2, 3);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 5);

        doReturn(Sets.newHashSet(tokenRange)).when(mockTokenMap).getTokenRanges();
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Set<DriverNode> nodes = replicationState.getNodesClusterWide(tableReference, subRange);

        assertThat(nodes).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);
    }

    @Test
    public void testGetNodesClusterWideForNonExistingSubRange() throws Exception
    {
        LongTokenRange subRange = new LongTokenRange(6, 7);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 5);

        doReturn(Sets.newHashSet(tokenRange)).when(mockTokenMap).getTokenRanges();
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        assertThat(replicationState.getNodesClusterWide(tableReference, subRange)).isNull();
    }

    @Test
    public void testGetNodesClusterWideForIntersectingSubRange() throws Exception
    {
        LongTokenRange subRange = new LongTokenRange(4, 7);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange existingRange = TokenUtil.getRange(1, 5);
        TokenRange existingRange2 = TokenUtil.getRange(5, 9);

        doReturn(Sets.newHashSet(existingRange, existingRange2)).when(mockTokenMap)
                .getTokenRanges();
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(existingRange));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(existingRange2));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        assertThat(replicationState.getNodesClusterWide(tableReference, subRange)).isNull();
    }

    @Test
    public void testGetTokenRanges() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        LongTokenRange range3 = new LongTokenRange(3, 4);
        LongTokenRange range4 = new LongTokenRange(4, 5);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange1 = TokenUtil.getRange(1, 2);
        TokenRange tokenRange2 = TokenUtil.getRange(2, 3);
        TokenRange tokenRange3 = TokenUtil.getRange(3, 4);
        TokenRange tokenRange4 = TokenUtil.getRange(4, 5);

        doReturn(Sets.newHashSet(tokenRange1, tokenRange2, tokenRange3, tokenRange4)).when(mockTokenMap)
                .getTokenRanges();
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange2));
        doReturn(Sets.newHashSet(mockReplica2, mockReplica3, mockReplica4)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange3));
        doReturn(Sets.newHashSet(mockReplica2, mockReplica3, mockReplica4)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange4));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Map<LongTokenRange, Set<DriverNode>> tokenRanges = replicationState.getTokenRanges(tableReference);

        assertThat(tokenRanges.keySet()).containsExactlyInAnyOrder(range1, range2, range3, range4);
        assertThat(tokenRanges.get(range1)).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);
        assertThat(tokenRanges.get(range2)).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);
        assertThat(tokenRanges.get(range3)).containsExactlyInAnyOrder(mockNode2, mockNode3, mockNode4);
        assertThat(tokenRanges.get(range4)).containsExactlyInAnyOrder(mockNode2, mockNode3, mockNode4);
    }

    @Test
    public void testGetTokenRangesReuse() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 2);

        doReturn(Sets.newHashSet(tokenRange)).when(mockTokenMap).getTokenRanges();
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockTokenMap)
                .getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockNodeResolver, mockSession, mockReplica1);

        Map<LongTokenRange, Set<DriverNode>> tokenRanges = replicationState.getTokenRanges(tableReference);

        assertThat(tokenRanges.keySet()).containsExactlyInAnyOrder(range1);
        assertThat(tokenRanges.get(range1)).containsExactlyInAnyOrder(mockNode1, mockNode2, mockNode3);

        assertThat(replicationState.getTokenRanges(tableReference)).isSameAs(tokenRanges);
    }
}
