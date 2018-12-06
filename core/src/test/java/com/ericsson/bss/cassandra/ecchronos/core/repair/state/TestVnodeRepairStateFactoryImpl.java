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
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestVnodeRepairStateFactoryImpl
{
    private static final TableReference TABLE_REFERENCE = new TableReference("ks", "tb");

    @Mock
    private ReplicationState mockReplicationState;

    @Test
    public void testEmptyHistoryNoPreviousIsUnrepaired() throws UnknownHostException
    {
        MockedHost host1 = new MockedHost("127.0.0.1");
        MockedHost host2 = new MockedHost("127.0.0.2");
        LongTokenRange longTokenRange1 = new LongTokenRange(1, 2);
        LongTokenRange longTokenRange2 = new LongTokenRange(2, 3);
        ImmutableSet<Host> replicas = getReplicas(host1, host2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenToHostMap = new HashMap<>();
        tokenToHostMap.put(longTokenRange1, replicas);
        tokenToHostMap.put(longTokenRange2, replicas);

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenToHostMap);
        RepairHistoryProvider repairHistoryProvider = new MockedRepairHistoryProvider(TABLE_REFERENCE);

        VnodeRepairStates expectedVnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange1, replicas, VnodeRepairState.UNREPAIRED))
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange2, replicas, VnodeRepairState.UNREPAIRED))
                .build();

        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider);

        VnodeRepairStates actualVnodeRepairStates = vnodeRepairStateFactory.calculateNewState(TABLE_REFERENCE, null);

        assertThat(actualVnodeRepairStates).isEqualTo(expectedVnodeRepairStates);
    }

    @Test
    public void testEmptyHistoryWithPreviousKeepsRepairedAt() throws UnknownHostException
    {
        MockedHost host1 = new MockedHost("127.0.0.1");
        MockedHost host2 = new MockedHost("127.0.0.2");
        LongTokenRange longTokenRange1 = new LongTokenRange(1, 2);
        LongTokenRange longTokenRange2 = new LongTokenRange(2, 3);
        ImmutableSet<Host> replicas = getReplicas(host1, host2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenToHostMap = new HashMap<>();
        tokenToHostMap.put(longTokenRange1, replicas);
        tokenToHostMap.put(longTokenRange2, replicas);

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenToHostMap);
        RepairHistoryProvider repairHistoryProvider = new MockedRepairHistoryProvider(TABLE_REFERENCE);

        VnodeRepairStates expectedVnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange1, replicas, 1))
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange2, replicas, 2))
                .build();

        RepairStateSnapshot previousRepairState = RepairStateSnapshot.newBuilder()
                .canRepair(false)
                .withLastRepairedAt(1)
                .withReplicaRepairGroup(null)
                .withVnodeRepairStates(expectedVnodeRepairStates)
                .build();

        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider);

        VnodeRepairStates actualVnodeRepairStates = vnodeRepairStateFactory.calculateNewState(TABLE_REFERENCE, previousRepairState);

        assertThat(actualVnodeRepairStates).isEqualTo(expectedVnodeRepairStates);
    }

    @Test
    public void testWithHistoryNoPreviousIsRepaired() throws UnknownHostException
    {
        MockedHost host1 = new MockedHost("127.0.0.1");
        MockedHost host2 = new MockedHost("127.0.0.2");

        LongTokenRange longTokenRange1 = new LongTokenRange(1, 2);
        LongTokenRange longTokenRange2 = new LongTokenRange(2, 3);
        ImmutableSet<Host> replicas = getReplicas(host1, host2);
        ImmutableSet<InetAddress> replicaAddresses = getReplicaAddresses(host1, host2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenToHostMap = new HashMap<>();
        tokenToHostMap.put(longTokenRange1, replicas);
        tokenToHostMap.put(longTokenRange2, replicas);

        long range1RepairedAt = 1;
        long range2RepairedAt = 2;

        RepairEntry repairEntry1 = new RepairEntry(longTokenRange1, range1RepairedAt, replicaAddresses, "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(longTokenRange2, range2RepairedAt, replicaAddresses, "SUCCESS");

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenToHostMap);
        RepairHistoryProvider repairHistoryProvider = new MockedRepairHistoryProvider(TABLE_REFERENCE, repairEntry1, repairEntry2);

        VnodeRepairStates expectedVnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange1, replicas, range1RepairedAt))
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange2, replicas, range2RepairedAt))
                .build();

        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider);

        VnodeRepairStates actualVnodeRepairStates = vnodeRepairStateFactory.calculateNewState(TABLE_REFERENCE, null);

        assertThat(actualVnodeRepairStates).isEqualTo(expectedVnodeRepairStates);
    }

    @Test
    public void testWithHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        MockedHost host1 = new MockedHost("127.0.0.1");
        MockedHost host2 = new MockedHost("127.0.0.2");

        LongTokenRange longTokenRange1 = new LongTokenRange(1, 2);
        LongTokenRange longTokenRange2 = new LongTokenRange(2, 3);
        ImmutableSet<Host> replicas = getReplicas(host1, host2);
        ImmutableSet<InetAddress> replicaAddresses = getReplicaAddresses(host1, host2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenToHostMap = new HashMap<>();
        tokenToHostMap.put(longTokenRange1, replicas);
        tokenToHostMap.put(longTokenRange2, replicas);

        long range1RepairedAt = 1;
        long range2RepairedAt = 2;

        RepairEntry repairEntry1 = new RepairEntry(longTokenRange1, range1RepairedAt, replicaAddresses, "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(longTokenRange2, range2RepairedAt, replicaAddresses, "FAILED");

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenToHostMap);
        RepairHistoryProvider repairHistoryProvider = new MockedRepairHistoryProvider(TABLE_REFERENCE, repairEntry1, repairEntry2);

        VnodeRepairStates expectedVnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange1, replicas, range1RepairedAt))
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange2, replicas, VnodeRepairState.UNREPAIRED))
                .build();

        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider);

        VnodeRepairStates actualVnodeRepairStates = vnodeRepairStateFactory.calculateNewState(TABLE_REFERENCE, null);

        assertThat(actualVnodeRepairStates).isEqualTo(expectedVnodeRepairStates);
    }

    @Test
    public void testWithOldHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        MockedHost host1 = new MockedHost("127.0.0.1");
        MockedHost host2 = new MockedHost("127.0.0.2");

        LongTokenRange longTokenRange1 = new LongTokenRange(1, 2);
        LongTokenRange longTokenRange2 = new LongTokenRange(2, 3);
        ImmutableSet<Host> replicas = getReplicas(host1, host2);
        ImmutableSet<InetAddress> replicaAddresses = getReplicaAddresses(host1, host2);

        Map<LongTokenRange, ImmutableSet<Host>> tokenToHostMap = new HashMap<>();
        tokenToHostMap.put(longTokenRange1, replicas);
        tokenToHostMap.put(longTokenRange2, replicas);

        long range1RepairedAt = 1;
        long range2RepairedAt = 2;

        RepairEntry repairEntry1 = new RepairEntry(longTokenRange1, range1RepairedAt, replicaAddresses, "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(longTokenRange2, range2RepairedAt, Collections.singleton(host1.getAddress()), "SUCCESS");

        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenToHostMap);
        RepairHistoryProvider repairHistoryProvider = new MockedRepairHistoryProvider(TABLE_REFERENCE, repairEntry1, repairEntry2);

        VnodeRepairStates expectedVnodeRepairStates = VnodeRepairStates.newBuilder()
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange1, replicas, range1RepairedAt))
                .combineVnodeRepairState(new VnodeRepairState(longTokenRange2, replicas, VnodeRepairState.UNREPAIRED))
                .build();

        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider);

        VnodeRepairStates actualVnodeRepairStates = vnodeRepairStateFactory.calculateNewState(TABLE_REFERENCE, null);

        assertThat(actualVnodeRepairStates).isEqualTo(expectedVnodeRepairStates);
    }

    private ImmutableSet<Host> getReplicas(MockedHost... hosts)
    {
        return ImmutableSet.copyOf(Lists.newArrayList(hosts).stream().map(MockedHost::getHost).collect(Collectors.toSet()));
    }

    private ImmutableSet<InetAddress> getReplicaAddresses(MockedHost... hosts)
    {
        return ImmutableSet.copyOf(Lists.newArrayList(hosts).stream().map(MockedHost::getAddress).collect(Collectors.toSet()));
    }

    private class MockedHost
    {
        private final Host myHost;
        private final InetAddress myInetAddress;

        public MockedHost(String inetAddress) throws UnknownHostException
        {
            myHost = mock(Host.class);
            myInetAddress = InetAddress.getByName(inetAddress);
            when(myHost.getBroadcastAddress()).thenReturn(myInetAddress);
        }

        public Host getHost()
        {
            return myHost;
        }

        public InetAddress getAddress()
        {
            return myInetAddress;
        }
    }

    private static class MockedRepairHistoryProvider implements RepairHistoryProvider
    {
        private final TableReference myTableReference;
        private final List<RepairEntry> myRepairEntries;

        public MockedRepairHistoryProvider(TableReference tableReference, RepairEntry... repairEntries)
        {
            myRepairEntries = Lists.newArrayList(repairEntries);
            myTableReference = tableReference;
        }

        @Override
        public Iterator<RepairEntry> iterate(TableReference tableReference, long to, Predicate<RepairEntry> predicate)
        {
            assertThat(tableReference).isEqualTo(myTableReference);

            return new MockedRepairEntryIterator(myRepairEntries.iterator(), predicate);
        }

        @Override
        public Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from, Predicate<RepairEntry> predicate)
        {
            assertThat(tableReference).isEqualTo(myTableReference);

            return new MockedRepairEntryIterator(myRepairEntries.iterator(), predicate);
        }
    }

    private static class MockedRepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final Iterator<RepairEntry> myBaseIterator;
        private final Predicate<RepairEntry> myPredicate;

        MockedRepairEntryIterator(Iterator<RepairEntry> baseIterator, Predicate<RepairEntry> predicate)
        {
            myBaseIterator = baseIterator;
            myPredicate = predicate;
        }

        @Override
        protected RepairEntry computeNext()
        {
            while(myBaseIterator.hasNext())
            {
                RepairEntry next = myBaseIterator.next();
                if (myPredicate.apply(next))
                {
                    return next;
                }
            }

            return endOfData();
        }
    }
}
