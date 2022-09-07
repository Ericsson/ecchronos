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

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestVnodeRepairStateFactoryImpl
{
    private static final TableReference TABLE_REFERENCE = tableReference("ks", "tb");

    @Mock
    private ReplicationState mockReplicationState;

    private Map<LongTokenRange, ImmutableSet<DriverNode>> tokenToNodeMap = new TreeMap<>(
            Comparator.comparingLong(l -> l.start));

    private RepairHistoryProvider repairHistoryProvider = new MockedRepairHistoryProvider(TABLE_REFERENCE);
    private List<RepairEntry> repairHistory = new ArrayList<>();

    @Before
    public void setup()
    {
        when(mockReplicationState.getTokenRangeToReplicas(eq(TABLE_REFERENCE))).thenReturn(tokenToNodeMap);
        when(mockReplicationState.getTokenRanges(eq(TABLE_REFERENCE))).thenReturn(tokenToNodeMap);
    }

    @Test
    public void testEmptyHistoryNoPreviousIsUnrepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");
 
        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        assertNewStateSameForVnodeAndSubrange(newUnrepairedState(range(1, 2)),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testCalculateStateEmptyHistoryIsUnrepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        assertStateSameForVnodeAndSubrange(1234L, 1235L, newUnrepairedState(range(1, 2)),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testCalculateStateClusterWideEmptyHistoryIsUnrepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        assertClusterWideStateSameForVnodeAndSubrange(1234L, 1235L, newUnrepairedState(range(1, 2)),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testEmptyHistoryWithPreviousKeepsRepairedAt() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        RepairStateSnapshot previousSnapshot = snapshot(1234L,
                newState(range(1, 2), 1234L, 1235L),
                newState(range(2, 3), 2345L, 2346L));

        assertNewVnodeStates(previousSnapshot,
                newState(range(1, 2), 1234L, 1235L),
                newState(range(2, 3), 2345L, 2346L));
        assertNewSubRangeStates(previousSnapshot, newState(range(1, 2), 1234L, -1L),
                newState(range(2, 3), 2345L, 2346L));
    }

    @Test
    public void testWithHistoryNoPreviousIsRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        withSuccessfulRepairHistory(range(1, 2), 1234L, 1235L);
        withSuccessfulRepairHistory(range(2, 3), 2345L, 2346L);

        assertNewStateSameForVnodeAndSubrange(newState(range(1, 2), 1234L, 1235L),
                newState(range(2, 3), 2345L, 2346L));
    }

    @Test
    public void testCalculateStateWithHistoryIsRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        withSuccessfulRepairHistory(range(1, 2), 1234L, 1235L);
        withSuccessfulRepairHistory(range(2, 3), 2345L, 2346L);

        assertStateSameForVnodeAndSubrange(2346L, 1234L,
                newState(range(1, 2), 1234L, 1235L),
                newState(range(2, 3), 2345L, 2346L));
    }

    @Test
    public void testCalculateClusterWideStateWithHistoryIsRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        withSuccessfulRepairHistory(range(1, 2), 1234L, 1235L);
        withSuccessfulRepairHistory(range(2, 3), 2345L, 2346L);

        assertClusterWideStateSameForVnodeAndSubrange(2346L, 1234L,
                newState(range(1, 2), 1234L, 1235L),
                newState(range(2, 3), 2345L, 2346L));
    }

    @Test
    public void testWithSubRangeHistoryNoPreviousIsRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 5), node1, node2);
        withRange(range(5, 10), node1, node2);

        long range1StartedAt = TimeUnit.DAYS.toMillis(10);
        long range1FinishedAt = TimeUnit.DAYS.toMillis(11);
        long range2StartedAt = TimeUnit.DAYS.toMillis(11);
        long range2FinishedAt = TimeUnit.DAYS.toMillis(12);

        withSubRangeSuccessfulRepairHistory(range(1, 3), range1StartedAt, range1FinishedAt);
        withSubRangeSuccessfulRepairHistory(range(3, 5), range1StartedAt, range1FinishedAt);

        withSubRangeSuccessfulRepairHistory(range(5, 8), range2StartedAt, range2FinishedAt);
        withSubRangeSuccessfulRepairHistory(range(8, 10), range2StartedAt, range2FinishedAt);

        assertNewVnodeStates(newUnrepairedState(range(1, 5)),
                newUnrepairedState(range(5, 10)));
        assertNewSubRangeStates(newState(range(1, 5), range1StartedAt, range1FinishedAt, (range1FinishedAt-range1StartedAt) + (range1FinishedAt-range1StartedAt)),
                newState(range(5, 10), range2StartedAt, range2FinishedAt, (range2FinishedAt-range2StartedAt) + (range2FinishedAt-range2StartedAt)));
    }

    @Test
    public void testWithSubRangeHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 5), node1, node2);
        withRange(range(5, 10), node1, node2);

        long range1StartedAt = TimeUnit.DAYS.toMillis(10);
        long range1FinishedAt = TimeUnit.DAYS.toMillis(10);
        long range2StartedAt = TimeUnit.DAYS.toMillis(11);
        long range2FinishedAt = TimeUnit.DAYS.toMillis(11);

        withSubRangeSuccessfulRepairHistory(range(1, 3), range1StartedAt, range1FinishedAt);

        withSubRangeSuccessfulRepairHistory(range(5, 8), range2StartedAt, range2FinishedAt);
        withSubRangeSuccessfulRepairHistory(range(8, 10), range2StartedAt, range2FinishedAt);

        assertNewVnodeStates(newUnrepairedState(range(1, 5)),
                newUnrepairedState(range(5, 10)));
        assertNewSubRangeStates(newSubRangeState(range(1, 3), range1StartedAt, range1FinishedAt),
                newSubRangeUnrepairedState(range(3, 5)),
                newState(range(5, 10), range2StartedAt, range2FinishedAt));
    }

    @Test
    public void testCalculateStateWithSubRangeHistoryIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 5), node1, node2);
        withRange(range(5, 10), node1, node2);

        long range1StartedAt = TimeUnit.DAYS.toMillis(10);
        long range1FinishedAt = TimeUnit.DAYS.toMillis(10);
        long range2StartedAt = TimeUnit.DAYS.toMillis(11);
        long range2FinishedAt = TimeUnit.DAYS.toMillis(11);

        withSubRangeSuccessfulRepairHistory(range(1, 3), range1StartedAt, range1FinishedAt);

        withSubRangeSuccessfulRepairHistory(range(5, 8), range2StartedAt, range2FinishedAt);
        withSubRangeSuccessfulRepairHistory(range(8, 10), range2StartedAt, range2FinishedAt);

        assertVnodeStates(range1StartedAt, range2FinishedAt, newUnrepairedState(range(1, 5)),
                newUnrepairedState(range(5, 10)));
        assertSubRangeStates(range2FinishedAt, range1StartedAt,
                newSubRangeState(range(1, 3), range1StartedAt, range1FinishedAt),
                newSubRangeUnrepairedState(range(3, 5)),
                newState(range(5, 10), range2StartedAt, range2FinishedAt));
    }

    @Test
    public void testCalculateClusterWideStateWithSubRangeHistoryIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 5), node1, node2);
        withRange(range(5, 10), node1, node2);

        long range1StartedAt = TimeUnit.DAYS.toMillis(10);
        long range1FinishedAt = TimeUnit.DAYS.toMillis(10);
        long range2StartedAt = TimeUnit.DAYS.toMillis(11);
        long range2FinishedAt = TimeUnit.DAYS.toMillis(11);

        withSubRangeSuccessfulRepairHistory(range(1, 3), range1StartedAt, range1FinishedAt);

        withSubRangeSuccessfulRepairHistory(range(5, 8), range2StartedAt, range2FinishedAt);
        withSubRangeSuccessfulRepairHistory(range(8, 10), range2StartedAt, range2FinishedAt);

        assertClusterWideVnodeStates(range1StartedAt, range2FinishedAt, newUnrepairedState(range(1, 5)),
                newUnrepairedState(range(5, 10)));
        assertClusterWideSubRangeStates(range2FinishedAt, range1StartedAt,
                newSubRangeState(range(1, 3), range1StartedAt, range1FinishedAt),
                newSubRangeUnrepairedState(range(3, 5)),
                newState(range(5, 10), range2StartedAt, range2FinishedAt));
    }

    @Test
    public void testWithSubRangeHistoryAndPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        long firstStartedAt = TimeUnit.DAYS.toMillis(8);
        long firstFinishedAt = TimeUnit.DAYS.toMillis(8);
        long range1StartedAt = TimeUnit.DAYS.toMillis(10);
        long range1FinishedAt = TimeUnit.DAYS.toMillis(10);
        long range2StartedAt = TimeUnit.DAYS.toMillis(11);
        long range2FinishedAt = TimeUnit.DAYS.toMillis(11);

        withRange(range(1, 5), node1, node2);
        withRange(range(5, 10), node1, node2);

        withSubRangeSuccessfulRepairHistory(range(1, 3), range1StartedAt, range1FinishedAt);

        withSubRangeSuccessfulRepairHistory(range(5, 8), range2StartedAt, range2FinishedAt);
        withSubRangeSuccessfulRepairHistory(range(8, 10), range2StartedAt, range2FinishedAt);

        RepairStateSnapshot previousSnapshot = snapshot(firstStartedAt,
                newState(range(1, 5), firstStartedAt, firstFinishedAt),
                newState(range(5, 10), firstStartedAt, firstFinishedAt));

        assertNewVnodeStates(previousSnapshot,
                newState(range(1, 5), firstStartedAt, firstFinishedAt),
                newState(range(5, 10), firstStartedAt, firstFinishedAt));

        assertNewSubRangeStates(previousSnapshot,
                newSubRangeState(range(1, 3), range1StartedAt, range1FinishedAt),
                newSubRangeState(range(3, 5), firstStartedAt, VnodeRepairState.UNREPAIRED),
                newSubRangeState(range(5, 10), range2StartedAt, range2FinishedAt));
    }

    @Test
    public void testWithHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        long range1StartedAt = 1;
        long range1FinishedAt = 2;
        long range2StartedAt = 3;

        withSuccessfulRepairHistory(range(1, 2), range1StartedAt, range1FinishedAt);
        withFailedRepairHistory(range(2, 3), range2StartedAt);

        assertNewStateSameForVnodeAndSubrange(newState(range(1, 2), range1StartedAt, range1FinishedAt),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testCalculateStateWithHistoryIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        long range1StartedAt = 1;
        long range1FinishedAt = 2;
        long range2StartedAt = 3;

        withSuccessfulRepairHistory(range(1, 2), range1StartedAt, range1FinishedAt);
        withFailedRepairHistory(range(2, 3), range2StartedAt);

        assertStateSameForVnodeAndSubrange(range2StartedAt, range1StartedAt, newState(range(1, 2), range1StartedAt, range1FinishedAt),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testCalculateClusterWideStateWithHistoryIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node2);

        long range1StartedAt = 1;
        long range1FinishedAt = 2;
        long range2StartedAt = 3;

        withSuccessfulRepairHistory(range(1, 2), range1StartedAt, range1FinishedAt);
        withFailedRepairHistory(range(2, 3), range2StartedAt);

        assertClusterWideStateSameForVnodeAndSubrange(range2StartedAt, range1StartedAt, newState(range(1, 2),
                        range1StartedAt, range1FinishedAt), newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testWithOldHistoryNoPreviousIsPartiallyRepaired() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");
        DriverNode node3 = withNode("127.0.0.3");

        long range1StartedAt = 1;
        long range1FinishedAt = 2;
        long range2StartedAt = 3;
        long range2FinishedAt = 4;

        withRange(range(1, 2), node1, node2);
        withRange(range(2, 3), node1, node3);
        withSuccessfulRepairHistory(range(2, 3), range2StartedAt, range2FinishedAt); // Previous replication

        replaceRange(range(2, 3), range(2, 3), node1, node2);

        withSuccessfulRepairHistory(range(1, 2), range1StartedAt, range1FinishedAt);

        assertNewStateSameForVnodeAndSubrange(newState(range(1, 2), range1StartedAt, range1FinishedAt),
                newUnrepairedState(range(2, 3)));
    }

    @Test
    public void testWithHistoryAndPreviousAfterScaleOut() throws UnknownHostException
    {
        DriverNode node1 = withNode("127.0.0.1");
        DriverNode node2 = withNode("127.0.0.2");
        DriverNode node3 = withNode("127.0.0.3");

        withRange(range(1, 4), node1, node2);
        withRange(range(5, 0), node1, node2);

        replaceRange(range(1, 4), range(1, 2), node1, node3);

        RepairStateSnapshot previousSnapshot = snapshot(1234L,
                newState(range(1, 4), 1234L, 1235L),
                newState(range(5, 0), 1236L, 1237L));

        assertNewStateSameForVnodeAndSubrange(previousSnapshot,
                newState(range(1, 2), 1234L, VnodeRepairState.UNREPAIRED),
                newState(range(5, 0), 1236L, 1237L));
    }

    private RepairStateSnapshot snapshot(long repairedAt, VnodeRepairState... states)
    {
        return RepairStateSnapshot.newBuilder()
                .withLastCompletedAt(repairedAt)
                .withReplicaRepairGroups(Collections.emptyList())
                .withVnodeRepairStates(vnodeRepairStates(states))
                .build();
    }

    private VnodeRepairStates vnodeRepairStates(VnodeRepairState... states)
    {
        return VnodeRepairStatesImpl.newBuilder(Arrays.asList(states)).build();
    }

    private void withSubRangeSuccessfulRepairHistory(LongTokenRange range, long startedAt, long finishedAt)
    {
        ImmutableSet<DriverNode> replicas = getKnownReplicasForSubRange(range);
        withRepairHistory(range, startedAt, finishedAt, replicas, "SUCCESS");
    }

    private void withSuccessfulRepairHistory(LongTokenRange range, long startedAt, long finishedAt)
    {
        ImmutableSet<DriverNode> replicas = getKnownReplicas(range);
        withRepairHistory(range, startedAt, finishedAt, replicas, "SUCCESS");
    }

    private void withFailedRepairHistory(LongTokenRange range, long startedAt)
    {
        ImmutableSet<DriverNode> replicas = getKnownReplicas(range);
        withRepairHistory(range, startedAt, VnodeRepairState.UNREPAIRED, replicas, "FAILED");
    }

    private void withRepairHistory(LongTokenRange range, long startedAt, long finishedAt, ImmutableSet<DriverNode> replicas, String status)
    {
        RepairEntry repairEntry = new RepairEntry(range, startedAt, finishedAt, replicas, status);
        repairHistory.add(repairEntry);
    }

    private VnodeRepairState newUnrepairedState(LongTokenRange range)
    {
        return newState(range, VnodeRepairState.UNREPAIRED, VnodeRepairState.UNREPAIRED);
    }

    private VnodeRepairState newState(LongTokenRange range, long startedAt, long finishedAt)
    {
        return new VnodeRepairState(range, getKnownReplicas(range), startedAt, finishedAt);
    }

    private VnodeRepairState newState(LongTokenRange range, long startedAt, long finishedAt, long repairTime)
    {
        return new VnodeRepairState(range, getKnownReplicas(range), startedAt, finishedAt, repairTime);
    }

    private VnodeRepairState newSubRangeUnrepairedState(LongTokenRange range)
    {
        return newSubRangeState(range, VnodeRepairState.UNREPAIRED, VnodeRepairState.UNREPAIRED);
    }

    private VnodeRepairState newSubRangeState(LongTokenRange range, long startedAt, long finishedAt)
    {
        return new VnodeRepairState(range, getKnownReplicasForSubRange(range), startedAt, finishedAt);
    }

    private ImmutableSet<DriverNode> getKnownReplicasForSubRange(LongTokenRange range)
    {
        ImmutableSet<DriverNode> replicas = tokenToNodeMap.get(range);
        if (replicas == null)
        {
            for (LongTokenRange vnode : tokenToNodeMap.keySet())
            {
                if (vnode.isCovering(range))
                {
                    replicas = tokenToNodeMap.get(vnode);
                    break;
                }
            }

            assertThat(replicas).isNotNull();
        }

        return replicas;
    }

    private ImmutableSet<DriverNode> getKnownReplicas(LongTokenRange range)
    {
        ImmutableSet<DriverNode> replicas = tokenToNodeMap.get(range);
        assertThat(replicas).isNotNull();
        return replicas;
    }

    private LongTokenRange range(long start, long end)
    {
        return new LongTokenRange(start, end);
    }

    private void withRange(LongTokenRange range, DriverNode... replicas)
    {
        tokenToNodeMap.put(range, ImmutableSet.copyOf(replicas));
    }

    private void replaceRange(LongTokenRange previousRange, LongTokenRange newRange, DriverNode... newReplicas)
    {
        tokenToNodeMap.remove(previousRange);
        withRange(newRange, newReplicas);
    }

    private DriverNode withNode(String inetAddress) throws UnknownHostException
    {
        DriverNode node = mock(DriverNode.class);
        InetAddress nodeAddress = InetAddress.getByName(inetAddress);
        when(node.getPublicAddress()).thenReturn(nodeAddress);
        return node;
    }

    private void assertNewVnodeStates(VnodeRepairState... states)
    {
        assertNewVnodeStates(null, states);
    }

    private void assertNewVnodeStates(RepairStateSnapshot previous, VnodeRepairState... states)
    {
        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, false);
        assertNewState(vnodeRepairStateFactory, previous, VnodeRepairStatesImpl.class, states);
    }

    private void assertNewSubRangeStates(VnodeRepairState... states)
    {
        assertNewSubRangeStates(null, states);
    }

    private void assertNewSubRangeStates(RepairStateSnapshot previous, VnodeRepairState... states)
    {
        VnodeRepairStateFactory subRangeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, true);
        assertNewState(subRangeRepairStateFactory, previous, SubRangeRepairStates.class, states);
    }

    private void assertNewStateSameForVnodeAndSubrange(VnodeRepairState... states)
    {
        assertNewStateSameForVnodeAndSubrange(null, states);
    }

    private void assertNewStateSameForVnodeAndSubrange(RepairStateSnapshot previous, VnodeRepairState... states)
    {
        assertNewVnodeStates(previous, states);
        assertNewSubRangeStates(previous, states);
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

    private void assertClusterWideStateSameForVnodeAndSubrange(long to, long from, VnodeRepairState... states)
    {
        assertClusterWideVnodeStates(to, from, states);
        assertClusterWideSubRangeStates(to, from, states);
    }

    private void assertClusterWideVnodeStates(long to, long from, VnodeRepairState... states)
    {
        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, false);
        assertClusterWideState(vnodeRepairStateFactory, to, from, VnodeRepairStatesImpl.class, states);
    }

    private void assertClusterWideSubRangeStates(long to, long from, VnodeRepairState... states)
    {
        VnodeRepairStateFactory subRangeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, true);
        assertClusterWideState(subRangeRepairStateFactory, to, from, SubRangeRepairStates.class, states);
    }

    private void assertClusterWideState(VnodeRepairStateFactory factory, long to, long from, Class<? extends VnodeRepairStates> expectedClass, VnodeRepairState... expectedStates)
    {
        assertClusterWideState(factory, to, from, expectedClass, Arrays.asList(expectedStates));
    }

    private void assertClusterWideState(VnodeRepairStateFactory factory, long to, long from, Class<? extends VnodeRepairStates> expectedClass, Collection<VnodeRepairState> expectedStates)
    {
        VnodeRepairStates states = factory.calculateClusterWideState(TABLE_REFERENCE, to, from);
        assertThat(states).isInstanceOf(expectedClass);

        Collection<VnodeRepairState> vnodeRepairStates = states.getVnodeRepairStates();
        assertThat(vnodeRepairStates).containsOnlyElementsOf(expectedStates);
    }

    private void assertStateSameForVnodeAndSubrange(long to, long from, VnodeRepairState... states)
    {
        assertVnodeStates(to, from, states);
        assertSubRangeStates(to, from, states);
    }

    private void assertVnodeStates(long to, long from, VnodeRepairState... states)
    {
        VnodeRepairStateFactory vnodeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, false);
        assertState(vnodeRepairStateFactory, to, from, VnodeRepairStatesImpl.class, states);
    }

    private void assertSubRangeStates(long to, long from, VnodeRepairState... states)
    {
        VnodeRepairStateFactory subRangeRepairStateFactory = new VnodeRepairStateFactoryImpl(mockReplicationState, repairHistoryProvider, true);
        assertState(subRangeRepairStateFactory, to, from, SubRangeRepairStates.class, states);
    }

    private void assertState(VnodeRepairStateFactory factory, long to, long from, Class<? extends VnodeRepairStates> expectedClass, VnodeRepairState... expectedStates)
    {
        assertState(factory, to, from, expectedClass, Arrays.asList(expectedStates));
    }

    private void assertState(VnodeRepairStateFactory factory, long to, long from, Class<? extends VnodeRepairStates> expectedClass, Collection<VnodeRepairState> expectedStates)
    {
        VnodeRepairStates newStates = factory.calculateState(TABLE_REFERENCE, to, from);
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

            return new MockedRepairEntryIterator(repairHistory.iterator(), predicate, to, -1L);
        }

        @Override
        public Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from, Predicate<RepairEntry> predicate)
        {
            assertThat(tableReference).isEqualTo(myTableReference);

            return new MockedRepairEntryIterator(repairHistory.iterator(), predicate, to, from);
        }

        @Override
        public Iterator<RepairEntry> iterate(UUID nodeId, TableReference tableReference, long to, long from,
                Predicate<RepairEntry> predicate)
        {
            assertThat(tableReference).isEqualTo(myTableReference);

            return new MockedRepairEntryIterator(repairHistory.iterator(), predicate, to, from);
        }
    }

    private static class MockedRepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final Iterator<RepairEntry> myBaseIterator;
        private final Predicate<RepairEntry> myPredicate;
        private final long myTo;
        private final long myFrom;

        MockedRepairEntryIterator(Iterator<RepairEntry> baseIterator, Predicate<RepairEntry> predicate, long to, long from)
        {
            myBaseIterator = baseIterator;
            myPredicate = predicate;
            myTo = to;
            myFrom = from;
        }

        @Override
        protected RepairEntry computeNext()
        {
            while(myBaseIterator.hasNext())
            {
                RepairEntry next = myBaseIterator.next();
                if (next.getFinishedAt() <= myTo)
                {
                    if (myFrom != -1L)
                    {
                        if (next.getStartedAt() >= myFrom && myPredicate.apply(next))
                        {
                            return next;
                        }
                    }
                    else
                    {
                        if (myPredicate.apply(next))
                        {
                            return next;
                        }
                    }
                }
            }

            return endOfData();
        }
    }
}
