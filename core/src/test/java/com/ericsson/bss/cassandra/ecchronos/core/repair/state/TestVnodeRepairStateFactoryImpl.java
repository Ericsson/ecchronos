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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
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

    private Map<LongTokenRange, ImmutableSet<Host>> tokenToHostMap = new TreeMap<>((l1, l2) -> Long.compare(l1.start, l2.start));

    private RepairHistoryProvider repairHistoryProvider = new MockedRepairHistoryProvider(TABLE_REFERENCE);
    private List<RepairEntry> repairHistory = new ArrayList<>();

    @Before
    public void setup()
    {
        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenToHostMap);
    }

    @Test
    public void testEmptyHistoryNoPreviousIsUnrepaired() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");
 
        withRange(range(1, 2), host1, host2);
        withRange(range(2, 3), host1, host2);

        assertSameForVnodeAndSubrange(newUnrepairedState(range(1, 2)),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testEmptyHistoryWithPreviousKeepsRepairedAt() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");

        withRange(range(1, 2), host1, host2);
        withRange(range(2, 3), host1, host2);

        RepairStateSnapshot previousSnapshot = snapshot(1234L,
                newState(range(1, 2), 1234L),
                newState(range(2, 3), 2345L));

        assertSameForVnodeAndSubrange(previousSnapshot,
                newState(range(1, 2), 1234L),
                newState(range(2, 3), 2345L));
    }

    @Test
    public void testWithHistoryNoPreviousIsRepaired() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");

        withRange(range(1, 2), host1, host2);
        withRange(range(2, 3), host1, host2);

        withSuccessfulRepairHistory(range(1, 2), 1234L);
        withSuccessfulRepairHistory(range(2, 3), 2345L);

        assertSameForVnodeAndSubrange(newState(range(1, 2), 1234L),
                newState(range(2, 3), 2345L));
    }

    @Test
    public void testWithSubRangeHistoryNoPreviousIsRepaired() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");

        withRange(range(1, 5), host1, host2);
        withRange(range(5, 10), host1, host2);

        long range1RepairedAt = TimeUnit.DAYS.toMillis(10);
        long range2RepairedAt = TimeUnit.DAYS.toMillis(11);

        withSubRangeSuccessfulRepairHistory(range(1, 3), range1RepairedAt);
        withSubRangeSuccessfulRepairHistory(range(3, 5), range1RepairedAt);

        withSubRangeSuccessfulRepairHistory(range(5, 8), range2RepairedAt);
        withSubRangeSuccessfulRepairHistory(range(8, 10), range2RepairedAt);

        assertVnodeStates(newUnrepairedState(range(1, 5)),
                newUnrepairedState(range(5, 10)));
        assertSubRangeStates(newState(range(1, 5), range1RepairedAt),
                newState(range(5, 10), range2RepairedAt));
    }

    @Test
    public void testWithSubRangeHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");

        withRange(range(1, 5), host1, host2);
        withRange(range(5, 10), host1, host2);

        long range1RepairedAt = TimeUnit.DAYS.toMillis(10);
        long range2RepairedAt = TimeUnit.DAYS.toMillis(11);

        withSubRangeSuccessfulRepairHistory(range(1, 3), range1RepairedAt);

        withSubRangeSuccessfulRepairHistory(range(5, 8), range2RepairedAt);
        withSubRangeSuccessfulRepairHistory(range(8, 10), range2RepairedAt);

        assertVnodeStates(newUnrepairedState(range(1, 5)),
                newUnrepairedState(range(5, 10)));
        assertSubRangeStates(newSubRangeState(range(1, 3), range1RepairedAt),
                newSubRangeUnrepairedState(range(3, 5)),
                newState(range(5, 10), range2RepairedAt));
    }

    @Test
    public void testWithHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");

        withRange(range(1, 2), host1, host2);
        withRange(range(2, 3), host1, host2);

        long range1RepairedAt = 1;
        long range2RepairedAt = 2;

        withSuccessfulRepairHistory(range(1, 2), range1RepairedAt);
        withFailedRepairHistory(range(2, 3), range2RepairedAt);

        assertSameForVnodeAndSubrange(newState(range(1, 2), range1RepairedAt),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testWithOldHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");
        Host host3 = withHost("127.0.0.3");

        long range1RepairedAt = 1;
        long range2RepairedAt = 2;

        withRange(range(1, 2), host1, host2);
        withRange(range(2, 3), host1, host3);
        withSuccessfulRepairHistory(range(2, 3), range2RepairedAt); // Previous replication

        replaceRange(range(2, 3), range(2, 3), host1, host2);

        withSuccessfulRepairHistory(range(1, 2), range1RepairedAt);

        assertSameForVnodeAndSubrange(newState(range(1, 2), range1RepairedAt),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testWithHistoryAndPreviousAfterScaleOut() throws UnknownHostException
    {
        Host host1 = withHost("127.0.0.1");
        Host host2 = withHost("127.0.0.2");
        Host host3 = withHost("127.0.0.3");

        withRange(range(1, 4), host1, host2);
        withRange(range(5, 0), host1, host2);

        replaceRange(range(1, 4), range(1, 2), host1, host3);

        RepairStateSnapshot previousSnapshot = snapshot(1234L,
                newState(range(1, 4), 1234L),
                newState(range(5, 0), 1234L));

        assertSameForVnodeAndSubrange(previousSnapshot,
                newState(range(1, 2), 1234L),
                newState(range(5, 0), 1234L));
    }

    private RepairStateSnapshot snapshot(long repairedAt, VnodeRepairState... states)
    {
        return RepairStateSnapshot.newBuilder()
                .withLastRepairedAt(repairedAt)
                .withReplicaRepairGroups(Collections.emptyList())
                .withVnodeRepairStates(vnodeRepairStates(states))
                .build();
    }

    private VnodeRepairStates vnodeRepairStates(VnodeRepairState... states)
    {
        return VnodeRepairStatesImpl.newBuilder(Arrays.asList(states)).build();
    }

    private void withSubRangeSuccessfulRepairHistory(LongTokenRange range, long repairedAt)
    {
        ImmutableSet<Host> replicas = getKnownReplicasForSubRange(range);
        withRepairHistory(range, repairedAt, replicas, "SUCCESS");
    }

    private void withSuccessfulRepairHistory(LongTokenRange range, long repairedAt)
    {
        ImmutableSet<Host> replicas = getKnownReplicas(range);
        withRepairHistory(range, repairedAt, replicas, "SUCCESS");
    }

    private void withFailedRepairHistory(LongTokenRange range, long repairedAt)
    {
        ImmutableSet<Host> replicas = getKnownReplicas(range);
        withRepairHistory(range, repairedAt, replicas, "FAILED");
    }

    private void withRepairHistory(LongTokenRange range, long repairedAt, ImmutableSet<Host> replicas, String status)
    {
        RepairEntry repairEntry = new RepairEntry(range, repairedAt, getReplicaAddresses(replicas), status);
        repairHistory.add(repairEntry);
    }

    private ImmutableSet<InetAddress> getReplicaAddresses(Collection<Host> hosts)
    {
        return ImmutableSet.copyOf(hosts.stream().map(Host::getBroadcastAddress).collect(Collectors.toSet()));
    }

    private VnodeRepairState newUnrepairedState(LongTokenRange range)
    {
        return newState(range, VnodeRepairState.UNREPAIRED);
    }

    private VnodeRepairState newState(LongTokenRange range, long repairedAt)
    {
        return new VnodeRepairState(range, getKnownReplicas(range), repairedAt);
    }

    private VnodeRepairState newSubRangeUnrepairedState(LongTokenRange range)
    {
        return newSubRangeState(range, VnodeRepairState.UNREPAIRED);
    }

    private VnodeRepairState newSubRangeState(LongTokenRange range, long repairedAt)
    {
        return new VnodeRepairState(range, getKnownReplicasForSubRange(range), repairedAt);
    }

    private ImmutableSet<Host> getKnownReplicasForSubRange(LongTokenRange range)
    {
        ImmutableSet<Host> replicas = tokenToHostMap.get(range);
        if (replicas == null)
        {
            for (LongTokenRange vnode : tokenToHostMap.keySet())
            {
                if (vnode.isCovering(range))
                {
                    replicas = tokenToHostMap.get(vnode);
                    break;
                }
            }

            assertThat(replicas).isNotNull();
        }

        return replicas;
    }

    private ImmutableSet<Host> getKnownReplicas(LongTokenRange range)
    {
        ImmutableSet<Host> replicas = tokenToHostMap.get(range);
        assertThat(replicas).isNotNull();
        return replicas;
    }

    private LongTokenRange range(long start, long end)
    {
        return new LongTokenRange(start, end);
    }

    private void withRange(LongTokenRange range, Host... replicas)
    {
        tokenToHostMap.put(range, ImmutableSet.copyOf(replicas));
    }

    private void replaceRange(LongTokenRange previousRange, LongTokenRange newRange, Host... newReplicas)
    {
        tokenToHostMap.remove(previousRange);
        withRange(newRange, newReplicas);
    }

    private Host withHost(String inetAddress) throws UnknownHostException
    {
        Host host = mock(Host.class);
        InetAddress hostAddress = InetAddress.getByName(inetAddress);
        when(host.getBroadcastAddress()).thenReturn(hostAddress);
        return host;
    }

    private void assertVnodeStates(VnodeRepairState... states)
    {
        assertVnodeStates(null, states);
    }

    private void assertVnodeStates(RepairStateSnapshot previous, VnodeRepairState... states)
    {
        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, false);
        assertNewState(vnodeRepairStateFactory, previous, VnodeRepairStatesImpl.class, states);
    }

    private void assertSubRangeStates(VnodeRepairState... states)
    {
        assertSubRangeStates(null, states);
    }

    private void assertSubRangeStates(RepairStateSnapshot previous, VnodeRepairState... states)
    {
        VnodeRepairStateFactory subRangeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, true);
        assertNewState(subRangeRepairStateFactory, previous, SubRangeRepairStates.class, states);
    }

    private void assertSameForVnodeAndSubrange(VnodeRepairState... states)
    {
        assertSameForVnodeAndSubrange(null, states);
    }

    private void assertSameForVnodeAndSubrange(RepairStateSnapshot previous, VnodeRepairState... states)
    {
        assertVnodeStates(previous, states);
        assertSubRangeStates(previous, states);
    }

    private void assertNewState(VnodeRepairStateFactory factory, RepairStateSnapshot previous, Class<? extends VnodeRepairStates> expectedClass, VnodeRepairState... expectedStates)
    {
        assertNewState(factory, previous, expectedClass, Arrays.asList(expectedStates));
    }

    private void assertNewState(VnodeRepairStateFactory factory, RepairStateSnapshot previous, Class<? extends VnodeRepairStates> expectedClass, Collection<VnodeRepairState> expectedStates)
    {
        VnodeRepairStates newStates = factory.calculateNewState(TABLE_REFERENCE, previous);
        assertThat(newStates).isInstanceOf(expectedClass);

        Collection<VnodeRepairState> vnodeRepairStates = newStates.getVnodeRepairStates();
        assertThat(vnodeRepairStates).containsOnlyElementsOf(expectedStates);
    }

    private class MockedRepairHistoryProvider implements RepairHistoryProvider
    {
        private final TableReference myTableReference;

        public MockedRepairHistoryProvider(TableReference tableReference)
        {
            myTableReference = tableReference;
        }

        @Override
        public Iterator<RepairEntry> iterate(TableReference tableReference, long to, Predicate<RepairEntry> predicate)
        {
            assertThat(tableReference).isEqualTo(myTableReference);

            return new MockedRepairEntryIterator(repairHistory.iterator(), predicate);
        }

        @Override
        public Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from, Predicate<RepairEntry> predicate)
        {
            assertThat(tableReference).isEqualTo(myTableReference);

            return new MockedRepairEntryIterator(repairHistory.iterator(), predicate);
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
