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
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestIncrementalRepairGroupFactory
{
    private static final TableReference TABLE_REFERENCE = new TableReference("ks", "tb");

    @Mock
    private ReplicationState mockReplicationState;

    @Test
    public void testNoRepairableVnodesAndNoState()
    {
        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(Collections.emptyMap());
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();

        IncrementalRepairGroupFactory repairGroupFactory = new IncrementalRepairGroupFactory(TABLE_REFERENCE, mockReplicationState);
        List<ReplicaRepairGroup> replicaRepairGroups = repairGroupFactory.generateReplicaRepairGroups(vnodeRepairStates);

        assertThat(replicaRepairGroups).isEmpty();
    }

    @Test
    public void testNoRepairableVnodes()
    {
        Host host1 = mockHost("dc1");
        Host host2 = mockHost("dc1");
        LongTokenRange longTokenRange = new LongTokenRange(1, 2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToHostMap = new HashMap<>();
        tokenRangeToHostMap.put(longTokenRange, ImmutableSet.of(host1, host2));

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenRangeToHostMap);
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();

        IncrementalRepairGroupFactory repairGroupFactory = new IncrementalRepairGroupFactory(TABLE_REFERENCE, mockReplicationState);
        List<ReplicaRepairGroup> replicaRepairGroups = repairGroupFactory.generateReplicaRepairGroups(vnodeRepairStates);

        assertThat(replicaRepairGroups).isEmpty();
    }

    @Test
    public void testMatchingRepairableVnodesAndState()
    {
        Host host1 = mockHost("dc1");
        Host host2 = mockHost("dc1");
        LongTokenRange longTokenRange = new LongTokenRange(1, 2);
        ImmutableSet<Host> replicas = ImmutableSet.of(host1, host2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToHostMap = new HashMap<>();
        tokenRangeToHostMap.put(longTokenRange, replicas);

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenRangeToHostMap);
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();
        vnodeRepairStates.add(new VnodeRepairState(longTokenRange, replicas, VnodeRepairState.UNREPAIRED));

        IncrementalRepairGroupFactory repairGroupFactory = new IncrementalRepairGroupFactory(TABLE_REFERENCE, mockReplicationState);
        List<ReplicaRepairGroup> replicaRepairGroups = repairGroupFactory.generateReplicaRepairGroups(vnodeRepairStates);

        ReplicaRepairGroup expectedReplicaRepairGroup = new ReplicaRepairGroup(replicas, Collections.singletonList(longTokenRange));

        assertThat(replicaRepairGroups).containsExactly(expectedReplicaRepairGroup);
    }

    @Test
    public void testNonMatchingRepairableVnodesAndState()
    {
        Host host1 = mockHost("dc1");
        Host host2 = mockHost("dc1");
        LongTokenRange longTokenRange1 = new LongTokenRange(1, 2);
        LongTokenRange longTokenRange2 = new LongTokenRange(2, 3);
        ImmutableSet<Host> replicas = ImmutableSet.of(host1, host2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToHostMap = new HashMap<>();
        tokenRangeToHostMap.put(longTokenRange1, replicas);
        tokenRangeToHostMap.put(longTokenRange2, replicas);

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenRangeToHostMap);
        List<VnodeRepairState> vnodeRepairStates = new ArrayList<>();
        vnodeRepairStates.add(new VnodeRepairState(longTokenRange1, replicas, VnodeRepairState.UNREPAIRED));

        IncrementalRepairGroupFactory repairGroupFactory = new IncrementalRepairGroupFactory(TABLE_REFERENCE, mockReplicationState);
        List<ReplicaRepairGroup> replicaRepairGroups = repairGroupFactory.generateReplicaRepairGroups(vnodeRepairStates);

        assertThat(replicaRepairGroups).isEmpty();
    }

    private Host mockHost(String dc)
    {
        Host host = mock(Host.class);
        when(host.getDatacenter()).thenReturn(dc);
        return host;
    }
}
