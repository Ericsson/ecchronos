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
import com.google.common.collect.Sets;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class TestFullyRepairedRepairEntryPredicate
{
    private static final String UNKNOWN = "UNKNOWN";
    private static final String STARTED = "STARTED";
    private static final String FAILED = "FAILED";
    private static final String SUCCESS = "SUCCESS";

    @Test
    public void testAcceptUnknown() throws UnknownHostException
    {
        assertThat(applyWith(UNKNOWN)).isFalse();
    }

    @Test
    public void testAcceptFailed() throws UnknownHostException
    {
        assertThat(applyWith(FAILED)).isFalse();
    }

    @Test
    public void testAcceptStarted() throws UnknownHostException
    {
        assertThat(applyWith(STARTED)).isFalse();
    }

    @Test
    public void testAcceptSuccess() throws UnknownHostException
    {
        assertThat(applyWith(SUCCESS)).isTrue();
    }

    @Test
    public void testAcceptSuccessMultipleRanges() throws UnknownHostException
    {
        List<LongTokenRange> expectedRepairedTokenRanges = new ArrayList<>();
        Set<InetAddress> expectedRepairedNodeAddresses = Sets.newHashSet(InetAddress.getLocalHost());
        Set<Node> expectedRepairedNodes = expectedRepairedNodeAddresses.stream().map(this::mockNode).collect(Collectors.toSet());

        Map<LongTokenRange, Collection<Node>> tokenToNodeMap = new HashMap<>();

        for (int i = 0; i < 5; i++)
        {
            LongTokenRange longTokenRange = new LongTokenRange(i, i + 1);
            expectedRepairedTokenRanges.add(longTokenRange);
            tokenToNodeMap.put(longTokenRange, expectedRepairedNodes);
        }

        for (LongTokenRange expectedRepairedTokenRange : expectedRepairedTokenRanges)
        {
            assertThat(applyWith(expectedRepairedTokenRange, tokenToNodeMap, expectedRepairedNodes, SUCCESS)).isTrue();
        }
    }

    @Test
    public void testAcceptPartialSuccess() throws UnknownHostException
    {
        LongTokenRange expectedRepairedTokenRange = new LongTokenRange(0, 1);
        Node repairedNode = mockNode(InetAddress.getLocalHost());
        Set<Node> repairedNodes = Sets.newHashSet(repairedNode);
        Set<Node> allNodes = new HashSet<>();
        allNodes.add(repairedNode);
        allNodes.add(mockNode(mock(InetAddress.class)));

        Map<LongTokenRange, Collection<Node>> tokenToNodeMap = new HashMap<>();
        tokenToNodeMap.put(expectedRepairedTokenRange, allNodes);

        assertThat(applyWith(expectedRepairedTokenRange, tokenToNodeMap, repairedNodes, SUCCESS)).isFalse();
    }

    @Test
    public void testAcceptNonExistingTokenRangeSuccess() throws UnknownHostException
    {
        LongTokenRange repairedTokenRange = new LongTokenRange(0, 1);
        LongTokenRange actualTokenRange = new LongTokenRange(0, 2);
        Set<InetAddress> expectedRepairedNodeAddresses = Sets.newHashSet(InetAddress.getLocalHost());
        Set<Node> expectedRepairedNodes = expectedRepairedNodeAddresses.stream().map(this::mockNode).collect(Collectors.toSet());

        Map<LongTokenRange, Collection<Node>> tokenToNodeMap = new HashMap<>();
        tokenToNodeMap.put(actualTokenRange, expectedRepairedNodes);

        assertThat(applyWith(repairedTokenRange, tokenToNodeMap, expectedRepairedNodes, SUCCESS)).isFalse();
    }

    private boolean applyWith(String status) throws UnknownHostException
    {
        LongTokenRange expectedRepairedTokenRange = new LongTokenRange(0, 1);
        Set<InetAddress> expectedRepairedNodeAddresses = Sets.newHashSet(InetAddress.getLocalHost());
        Set<Node> expectedRepairedNodes = expectedRepairedNodeAddresses.stream().map(this::mockNode).collect(Collectors.toSet());

        Map<LongTokenRange, Collection<Node>> tokenToNodeMap = new HashMap<>();
        tokenToNodeMap.put(expectedRepairedTokenRange, expectedRepairedNodes);

        return applyWith(expectedRepairedTokenRange, tokenToNodeMap, expectedRepairedNodes, status);
    }

    private boolean applyWith(LongTokenRange repairedTokenRange, Map<LongTokenRange, Collection<Node>> tokenToNodeMap, Set<Node> repairedNodes, String status)
    {
        RepairEntry repairEntry = new RepairEntry(repairedTokenRange, 5, 5, repairedNodes, status);
        FullyRepairedRepairEntryPredicate fullyRepairedRepairEntryPredicate = new FullyRepairedRepairEntryPredicate(tokenToNodeMap);

        return fullyRepairedRepairEntryPredicate.apply(repairEntry);
    }

    private Node mockNode(InetAddress inetAddress)
    {
        Node node = mock(Node.class);

        when(node.getPublicAddress()).thenReturn(inetAddress);

        return node;
    }
}
