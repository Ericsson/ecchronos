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

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TokenSubRangeUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class TestVnodeRepairStates
{
    @Test
    public void testVnodeRepairStates()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        Node node1 = mock(Node.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), VnodeRepairState.UNREPAIRED);
        VnodeRepairState vnodeRepairState2 = new VnodeRepairState(range2, ImmutableSet.of(node1), VnodeRepairState.UNREPAIRED);

        List<VnodeRepairState> base = Arrays.asList(vnodeRepairState, vnodeRepairState2);
        List<VnodeRepairState> toUpdate = Collections.emptyList();

        assertVnodeRepairStatesContainsExactly(base, toUpdate, vnodeRepairState, vnodeRepairState2);
        assertSubRangeStatesContainsExactly(base, toUpdate, vnodeRepairState, vnodeRepairState2);
    }

    @Test
    public void testCombineMoreRecentlyRepaired()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        Node node1 = mock(Node.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), VnodeRepairState.UNREPAIRED);
        VnodeRepairState updatedVnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), 1234L);

        List<VnodeRepairState> base = Collections.singletonList(vnodeRepairState);
        List<VnodeRepairState> toUpdate = Arrays.asList(updatedVnodeRepairState, vnodeRepairState);

        assertVnodeRepairStatesContainsExactly(base, toUpdate, updatedVnodeRepairState);
        assertSubRangeStatesContainsExactly(base, toUpdate, updatedVnodeRepairState);
    }

    @Test
    public void testCombineNotSameNode()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        Node node1 = mock(Node.class);
        Node node2 = mock(Node.class);
        Node node3 = mock(Node.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1, node2), 1234L);
        VnodeRepairState updatedVnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1, node3), VnodeRepairState.UNREPAIRED);

        List<VnodeRepairState> base = Collections.singletonList(vnodeRepairState);
        List<VnodeRepairState> toUpdate = Collections.singletonList(updatedVnodeRepairState);

        assertVnodeRepairStatesContainsExactly(base, toUpdate, vnodeRepairState);
        assertSubRangeStatesContainsExactly(base, toUpdate, vnodeRepairState);
    }

    @Test
    public void testCombineNotSameRange()
    {
        LongTokenRange range = new LongTokenRange(1, 2);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        Node node1 = mock(Node.class);
        Node node2 = mock(Node.class);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1, node2), 1234L);
        VnodeRepairState updatedVnodeRepairState = new VnodeRepairState(range2, ImmutableSet.of(node1, node2), VnodeRepairState.UNREPAIRED);

        List<VnodeRepairState> base = Collections.singletonList(vnodeRepairState);
        List<VnodeRepairState> toUpdate = Collections.singletonList(updatedVnodeRepairState);

        assertVnodeRepairStatesContainsExactly(base, toUpdate, vnodeRepairState);
        assertSubRangeStatesContainsExactly(base, toUpdate, vnodeRepairState);
    }

    /**
     * Combining with an intersecting range should be ignored
     */
    @Test
    public void testCombineIntersectingRange()
    {
        LongTokenRange range = new LongTokenRange(1, 3);
        LongTokenRange range2 = new LongTokenRange(2, 4);
        Node node1 = mock(Node.class);
        Node node2 = mock(Node.class);

        long baseRepairedAt = 1234L;
        long updatedRepairedAt = baseRepairedAt + TimeUnit.HOURS.toMillis(2);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1, node2), 1234L);
        VnodeRepairState updatedVnodeRepairState = new VnodeRepairState(range2, ImmutableSet.of(node1, node2), updatedRepairedAt);

        List<VnodeRepairState> base = Collections.singletonList(vnodeRepairState);
        List<VnodeRepairState> toUpdate = Collections.singletonList(updatedVnodeRepairState);

        assertVnodeRepairStatesContainsExactly(base, toUpdate, vnodeRepairState);
        assertSubRangeStatesContainsExactly(base, toUpdate, vnodeRepairState);
    }

    /**
     * Test that base vnode (1, 5] that is repaired at X is updated with subranges:
     * (2, 4] repaired at X + 2h
     * (3, 5] repaired at X
     *
     * The result should be:
     * (1, 2] repaired at X
     * (2, 4] repaired at X + 2h
     * (4, 5] repaired at X
     */
    @Test
    public void testCombinePartialOverlappingRanges()
    {
        LongTokenRange range = new LongTokenRange(1, 5);
        LongTokenRange range2 = new LongTokenRange(2, 4);
        LongTokenRange range3 = new LongTokenRange(3, 5);
        Node node1 = mock(Node.class);
        Node node2 = mock(Node.class);

        long baseRepairedAt = 1234L;
        long updatedRepairedAt = baseRepairedAt + TimeUnit.HOURS.toMillis(2);

        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1, node2), baseRepairedAt);
        List<VnodeRepairState> expectedVnodeRepairStates = Arrays.asList(
                new VnodeRepairState(new LongTokenRange(1, 2), ImmutableSet.of(node1, node2), baseRepairedAt),
                new VnodeRepairState(new LongTokenRange(2, 4), ImmutableSet.of(node1, node2), updatedRepairedAt),
                new VnodeRepairState(new LongTokenRange(4, 5), ImmutableSet.of(node1, node2), baseRepairedAt)
        );

        VnodeRepairState updatedVnodeRepairState = new VnodeRepairState(range2, ImmutableSet.of(node1, node2), updatedRepairedAt);
        VnodeRepairState updatedVnodeRepairState2 = new VnodeRepairState(range3, ImmutableSet.of(node1, node2), baseRepairedAt);

        List<VnodeRepairState> base = Collections.singletonList(vnodeRepairState);
        List<VnodeRepairState> toUpdate = Arrays.asList(updatedVnodeRepairState, updatedVnodeRepairState2);

        assertVnodeRepairStatesContainsExactly(base, toUpdate, vnodeRepairState);
        assertSubRangeStatesContainsExactly(base, toUpdate, expectedVnodeRepairStates);
    }

    @Test
    public void testCombineSubRanges()
    {
        LongTokenRange range = new LongTokenRange(1, 100);
        Node node1 = mock(Node.class);

        VnodeRepairState expectedVnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), 1234L);
        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), VnodeRepairState.UNREPAIRED);

        List<VnodeRepairState> base = Collections.singletonList(vnodeRepairState);
        List<VnodeRepairState> toUpdate = generateSubRanges(range, 10, node1, 1234L);

        assertVnodeRepairStatesContainsExactly(base, toUpdate, vnodeRepairState);
        assertSubRangeStatesContainsExactly(base, toUpdate, expectedVnodeRepairState);
    }

    @Test
    public void testCombineSubRangesWrapAround()
    {
        LongTokenRange range = new LongTokenRange(50, -50);
        Node node1 = mock(Node.class);

        VnodeRepairState expectedVnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), 1234L);
        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), VnodeRepairState.UNREPAIRED);

        List<VnodeRepairState> base = Collections.singletonList(vnodeRepairState);
        List<VnodeRepairState> toUpdate = generateSubRanges(range, 10, node1, 1234L);

        assertVnodeRepairStatesContainsExactly(base, toUpdate, vnodeRepairState);
        assertSubRangeStatesContainsExactly(base, toUpdate, expectedVnodeRepairState);
    }

    @Test
    public void testCombineSubRangesWithSimilarTime()
    {
        LongTokenRange range = new LongTokenRange(1, 100);
        Node node1 = mock(Node.class);
        ImmutableSet<Node> nodeSet = ImmutableSet.of(node1);

        long totalRangeMs = LocalDateTime.parse("2020-03-12T14:00:00").toEpochSecond(ZoneOffset.UTC) * 1000;
        long subRange1Ms = LocalDateTime.parse("2020-03-13T14:00:05").toEpochSecond(ZoneOffset.UTC) * 1000;
        long subRange2Ms = LocalDateTime.parse("2020-03-13T14:30:05").toEpochSecond(ZoneOffset.UTC) * 1000;

        VnodeRepairState expectedVnodeRepairState = new VnodeRepairState(range, nodeSet, subRange1Ms);
        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), totalRangeMs);
        List<VnodeRepairState> subRangeRepairStates = new ArrayList<>();
        subRangeRepairStates.add(new VnodeRepairState(new LongTokenRange(1, 50), nodeSet, subRange1Ms));
        subRangeRepairStates.add(new VnodeRepairState(new LongTokenRange(50, 100), nodeSet, subRange2Ms));

        assertVnodeRepairStatesContainsExactly(Collections.singletonList(vnodeRepairState), subRangeRepairStates, vnodeRepairState);
        assertSubRangeStatesContainsExactly(Collections.singletonList(vnodeRepairState), subRangeRepairStates, expectedVnodeRepairState);
    }

    @Test
    public void testCombinePartialSubRangesWithSimilarTime()
    {
        LongTokenRange range = new LongTokenRange(1, 100);
        Node node1 = mock(Node.class);
        ImmutableSet<Node> nodeSet = ImmutableSet.of(node1);

        long totalRangeMs = LocalDateTime.parse("2020-03-12T14:00:00").toEpochSecond(ZoneOffset.UTC) * 1000;
        long subRange1Ms = LocalDateTime.parse("2020-03-13T14:00:05").toEpochSecond(ZoneOffset.UTC) * 1000;
        long subRange2Ms = LocalDateTime.parse("2020-03-13T14:30:05").toEpochSecond(ZoneOffset.UTC) * 1000;

        List<VnodeRepairState> expectedVnodeRepairStates = Arrays.asList(
                new VnodeRepairState(new LongTokenRange(1, 66), nodeSet, subRange1Ms), // Repaired sub ranges
                new VnodeRepairState(new LongTokenRange(66, 100), nodeSet, totalRangeMs) // The "rest"
        );
        VnodeRepairState vnodeRepairState = new VnodeRepairState(range, ImmutableSet.of(node1), totalRangeMs);
        List<VnodeRepairState> subRangeRepairStates = new ArrayList<>();
        subRangeRepairStates.add(new VnodeRepairState(new LongTokenRange(1, 33), nodeSet, subRange1Ms));
        subRangeRepairStates.add(new VnodeRepairState(new LongTokenRange(33, 66), nodeSet, subRange2Ms));

        assertVnodeRepairStatesContainsExactly(Collections.singletonList(vnodeRepairState), subRangeRepairStates, vnodeRepairState);
        assertSubRangeStatesContainsExactly(Collections.singletonList(vnodeRepairState), subRangeRepairStates, expectedVnodeRepairStates);
    }

    private List<VnodeRepairState> generateSubRanges(LongTokenRange range, int subRangeCount, Node node, long lastRepairedAt)
    {
        BigInteger fullRange = range.rangeSize();
        BigInteger tokensPerSubRange = fullRange.divide(BigInteger.valueOf(subRangeCount));

        List<LongTokenRange> subRanges = new TokenSubRangeUtil(range).generateSubRanges(tokensPerSubRange);

        return subRanges.stream()
                .map(subRange -> new VnodeRepairState(subRange, ImmutableSet.of(node), lastRepairedAt))
                .collect(Collectors.toList());
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(VnodeRepairStates.class)
                .withPrefabValues(ImmutableList.class, ImmutableList.of(1), ImmutableList.of(2))
                .usingGetClass()
                .verify();
    }

    private void assertSubRangeStatesContainsExactly(List<VnodeRepairState> base, List<VnodeRepairState> toUpdate, VnodeRepairState... expectedVnodeRepairStates)
    {
        assertSubRangeStatesContainsExactly(base, toUpdate, Arrays.asList(expectedVnodeRepairStates));
    }

    private void assertSubRangeStatesContainsExactly(List<VnodeRepairState> base, List<VnodeRepairState> toUpdate, List<VnodeRepairState> expectedVnodeRepairStates)
    {
        VnodeRepairStates actualVnodeRepairStates = SubRangeRepairStates.newBuilder(base)
                .updateVnodeRepairStates(toUpdate)
                .build();

        assertThat(actualVnodeRepairStates.getVnodeRepairStates()).containsOnlyElementsOf(expectedVnodeRepairStates);
    }

    private void assertVnodeRepairStatesContainsExactly(List<VnodeRepairState> base, List<VnodeRepairState> toUpdate, VnodeRepairState... expectedVnodeRepairStates)
    {
        assertVnodeRepairStatesContainsExactly(base, toUpdate, Arrays.asList(expectedVnodeRepairStates));
    }

    private void assertVnodeRepairStatesContainsExactly(List<VnodeRepairState> base, List<VnodeRepairState> toUpdate, List<VnodeRepairState> expectedVnodeRepairStates)
    {
        VnodeRepairStates actualVnodeRepairStates = VnodeRepairStatesImpl.newBuilder(base)
                .updateVnodeRepairStates(toUpdate)
                .build();

        assertThat(actualVnodeRepairStates.getVnodeRepairStates()).containsOnlyElementsOf(expectedVnodeRepairStates);
    }
}
