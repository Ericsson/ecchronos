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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.TokenUtil;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestReplicationStateImpl
{
    @Mock
    private Metadata mockMetadata;

    @Mock
    private Host mockReplica1;

    @Mock
    private Host mockReplica2;

    @Mock
    private Host mockReplica3;

    @Test
    public void testGetTokenRangeToReplicaSingleToken() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 2);

        doReturn(Sets.newHashSet(tokenRange)).when(mockMetadata).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockMetadata).getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockMetadata, mockReplica1);

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockReplica1, mockReplica2, mockReplica3);
    }

    @Test
    public void testGetTokenRangeToReplica() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange1 = TokenUtil.getRange(1, 2);
        TokenRange tokenRange2 = TokenUtil.getRange(2, 3);

        doReturn(Sets.newHashSet(tokenRange1, tokenRange2)).when(mockMetadata).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2)).when(mockMetadata).getReplicas(eq("ks"), eq(tokenRange1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica3)).when(mockMetadata).getReplicas(eq("ks"), eq(tokenRange2));

        ReplicationState replicationState = new ReplicationStateImpl(mockMetadata, mockReplica1);

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1, range2);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockReplica1, mockReplica2);
        assertThat(tokenRangeToReplicas.get(range2)).containsExactlyInAnyOrder(mockReplica1, mockReplica3);
    }

    @Test
    public void testGetTokenRangeToReplicaSetReuse() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange1 = TokenUtil.getRange(1, 2);
        TokenRange tokenRange2 = TokenUtil.getRange(2, 3);

        doReturn(Sets.newHashSet(tokenRange1, tokenRange2)).when(mockMetadata).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2)).when(mockMetadata).getReplicas(eq("ks"), eq(tokenRange1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2)).when(mockMetadata).getReplicas(eq("ks"), eq(tokenRange2));

        ReplicationState replicationState = new ReplicationStateImpl(mockMetadata, mockReplica1);

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1, range2);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockReplica1, mockReplica2);
        assertThat(tokenRangeToReplicas.get(range1)).isSameAs(tokenRangeToReplicas.get(range2));
    }

    @Test
    public void testGetTokenRangeToReplicaMapReuse() throws Exception
    {
        LongTokenRange range1 = new LongTokenRange(1, 2);
        TableReference tableReference = tableReference("ks", "tb");

        TokenRange tokenRange = TokenUtil.getRange(1, 2);

        doReturn(Sets.newHashSet(tokenRange)).when(mockMetadata).getTokenRanges(eq("ks"), eq(mockReplica1));
        doReturn(Sets.newHashSet(mockReplica1, mockReplica2, mockReplica3)).when(mockMetadata).getReplicas(eq("ks"), eq(tokenRange));

        ReplicationState replicationState = new ReplicationStateImpl(mockMetadata, mockReplica1);

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicas = replicationState.getTokenRangeToReplicas(tableReference);

        assertThat(tokenRangeToReplicas.keySet()).containsExactlyInAnyOrder(range1);
        assertThat(tokenRangeToReplicas.get(range1)).containsExactlyInAnyOrder(mockReplica1, mockReplica2, mockReplica3);

        assertThat(replicationState.getTokenRangeToReplicas(tableReference)).isSameAs(tokenRangeToReplicas);
    }
}
