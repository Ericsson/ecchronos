/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestVnodeRepairStateSummarizer
{
    @Mock
    Host mockHost;

    @Test
    public void summarizeSingleTokenNoPartial()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));
        List<VnodeRepairState> baseVnodes = Collections.singletonList(baseVnode);

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode);

        assertThat(actualVnodeRepairStates).isEqualTo(baseVnodes);
    }

    @Test
    public void summarizeSingleTokenFullPartial()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));
        VnodeRepairState partialVnode = withVnode(500, 3000, dateToTimestamp("2020-03-13T16:00:00"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode);

        assertThat(actualVnodeRepairStates).containsExactly(partialVnode);
    }

    @Test
    public void summarizeSingleTokenMultiplePartialSequential()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(500, 1800, dateToTimestamp("2020-03-13T15:30:00"));
        VnodeRepairState partialVnode2 = withVnode(1800, 2500, dateToTimestamp("2020-03-13T16:15:00"));
        VnodeRepairState partialVnode3 = withVnode(2500, 3000, dateToTimestamp("2020-03-13T16:29:59"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1, partialVnode3);

        assertThat(actualVnodeRepairStates).containsExactly(withVnode(500, 3000, dateToTimestamp("2020-03-13T15:30:00")));
    }

    @Test
    public void summarizeMultipleAdjacentTokenMultiplePartialSequential()
    {
        VnodeRepairState baseVnode1 = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));
        VnodeRepairState baseVnode2 = withVnode(3000, 5500, dateToTimestamp("2020-03-12T16:05:00"));

        // Base1
        VnodeRepairState partialVnode1 = withVnode(500, 1800, dateToTimestamp("2020-03-13T15:30:00"));
        VnodeRepairState partialVnode2 = withVnode(1800, 2500, dateToTimestamp("2020-03-13T16:15:00"));
        VnodeRepairState partialVnode3 = withVnode(2500, 3000, dateToTimestamp("2020-03-13T16:29:59"));

        // Base2
        VnodeRepairState partialVnode4 = withVnode(3000, 3800, dateToTimestamp("2020-03-13T15:45:00"));
        VnodeRepairState partialVnode5 = withVnode(3800, 4500, dateToTimestamp("2020-03-13T16:15:00"));
        VnodeRepairState partialVnode6 = withVnode(4500, 5500, dateToTimestamp("2020-03-13T16:29:59"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(Arrays.asList(baseVnode1, baseVnode2),
                partialVnode2, partialVnode1, partialVnode3, partialVnode4, partialVnode5, partialVnode6);

        assertThat(actualVnodeRepairStates).containsExactlyInAnyOrder(
                withVnode(500, 3000, dateToTimestamp("2020-03-13T15:30:00"))
                ,withVnode(3000, 5500, dateToTimestamp("2020-03-13T15:45:00"))
        );
    }

    @Test
    public void summarizeSingleTokenMultiplePartialSequentialMoreThanOneHour()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(500, 1800, dateToTimestamp("2020-03-13T15:30:00"));
        VnodeRepairState partialVnode2 = withVnode(1800, 2500, dateToTimestamp("2020-03-13T16:15:00"));
        VnodeRepairState partialVnode3 = withVnode(2500, 2800, dateToTimestamp("2020-03-13T16:30:01"));
        VnodeRepairState partialVnode4 = withVnode(2800, 3000, dateToTimestamp("2020-03-13T16:31:00"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1, partialVnode3, partialVnode4);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(500, 2500, dateToTimestamp("2020-03-13T15:30:00")),
                withVnode(2500, 3000, dateToTimestamp("2020-03-13T16:30:01"))
                );
    }

    @Test
    public void summarizeSingleTokenMultiplePartialNonSequential()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(600, 1800, dateToTimestamp("2020-03-13T16:00:00"));
        VnodeRepairState partialVnode2 = withVnode(2000, 2800, dateToTimestamp("2020-03-13T16:05:00"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(500, 600, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(600, 1800, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(1800, 2000, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(2000, 2800, dateToTimestamp("2020-03-13T16:05:00")),
                withVnode(2800, 3000, dateToTimestamp("2020-03-12T16:00:00"))
                );
    }

    @Test
    public void summarizeSingleTokenMultiplePartialOverlapping()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(600, 1800, dateToTimestamp("2020-03-13T16:00:00"));
        VnodeRepairState partialVnode2 = withVnode(1600, 2800, dateToTimestamp("2020-03-13T16:05:00"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(500, 600, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(600, 2800, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(2800, 3000, dateToTimestamp("2020-03-12T16:00:00"))
        );
    }

    @Test
    public void summarizeSingleTokenMultiplePartialOverlappingMoreThanOneHour()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(600, 1800, dateToTimestamp("2020-03-13T16:00:00"));
        VnodeRepairState partialVnode2 = withVnode(1600, 2800, dateToTimestamp("2020-03-13T17:00:01"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(500, 600, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(600, 1600, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(1600, 2800, dateToTimestamp("2020-03-13T17:00:01")),
                withVnode(2800, 3000, dateToTimestamp("2020-03-12T16:00:00"))
        );
    }

    @Test
    public void summarizeSingleTokenMultiplePartialOverlappingMoreThanOneHourBefore()
    {
        VnodeRepairState baseVnode = withVnode(500, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(600, 1800, dateToTimestamp("2020-03-13T17:00:01"));
        VnodeRepairState partialVnode2 = withVnode(1600, 2800, dateToTimestamp("2020-03-13T16:00:00"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(500, 600, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(600, 1800, dateToTimestamp("2020-03-13T17:00:01")),
                withVnode(1800, 2800, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(2800, 3000, dateToTimestamp("2020-03-12T16:00:00"))
        );
    }

    @Test
    public void summarizeSingleTokenMultiplePartialWrapAroundSequential()
    {
        VnodeRepairState baseVnode = withVnode(30000, 15000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(40000, 50000, dateToTimestamp("2020-03-13T16:00:00"));
        VnodeRepairState partialVnode2 = withVnode(-5000, 10000, dateToTimestamp("2020-03-13T16:05:00"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(30000, 40000, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(40000, 50000, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(50000, -5000, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(-5000, 10000, dateToTimestamp("2020-03-13T16:05:00")),
                withVnode(10000, 15000, dateToTimestamp("2020-03-12T16:00:00"))
        );
    }

    @Test
    public void summarizeSingleTokenMultiplePartialOverlappingWrapAround()
    {
        VnodeRepairState baseVnode = withVnode(5000, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(600, 1800, dateToTimestamp("2020-03-13T16:00:00"));
        VnodeRepairState partialVnode2 = withVnode(1600, 2800, dateToTimestamp("2020-03-13T16:05:00"));

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(5000, 600, dateToTimestamp("2020-03-12T16:00:00")),
                withVnode(600, 2800, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(2800, 3000, dateToTimestamp("2020-03-12T16:00:00"))
        );
    }

    @Test
    public void summarizeMultipleCoveringAndIntersectingRanges()
    {
        VnodeRepairState baseVnode = withVnode(100, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(100, 500, dateToTimestamp("2020-03-13T16:00:00")); // 100 -> 250
        VnodeRepairState partialVnode2 = withVnode(200, 300, dateToTimestamp("2020-03-12T16:05:00"));
        VnodeRepairState partialVnode3 = withVnode(250, 450, dateToTimestamp("2020-03-12T16:05:00"));
        VnodeRepairState partialVnode4 = withVnode(250, 600, dateToTimestamp("2020-03-13T17:05:00")); // 250 -> 600
        VnodeRepairState partialVnode5 = withVnode(400, 500, dateToTimestamp("2020-03-12T16:05:00"));
        VnodeRepairState partialVnode6 = withVnode(400, 3000, dateToTimestamp("2020-03-12T16:05:00")); // 600 -> 3000

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1, partialVnode3, partialVnode4, partialVnode5, partialVnode6);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(100, 250, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(250, 600, dateToTimestamp("2020-03-13T17:05:00")),
                withVnode(600, 3000, dateToTimestamp("2020-03-12T16:05:00"))
        );
    }

    @Test
    public void summarizeMultipleCoveringAndIntersectingRangesWithLaterRepairedAt()
    {
        VnodeRepairState baseVnode = withVnode(100, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(100, 500, dateToTimestamp("2020-03-13T16:00:00")); // 100 -> 200
        VnodeRepairState partialVnode2 = withVnode(200, 300, dateToTimestamp("2020-03-14T16:05:00")); // 200 -> 500 (1)
        VnodeRepairState partialVnode3 = withVnode(250, 450, dateToTimestamp("2020-03-14T16:05:00"));
        VnodeRepairState partialVnode4 = withVnode(250, 600, dateToTimestamp("2020-03-13T17:05:00")); // 500 -> 600
        VnodeRepairState partialVnode5 = withVnode(400, 500, dateToTimestamp("2020-03-14T16:05:00")); // 200 -> 500 (2)
        VnodeRepairState partialVnode6 = withVnode(400, 3000, dateToTimestamp("2020-03-13T16:05:00")); // 600 -> 3000

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1, partialVnode3, partialVnode4, partialVnode5, partialVnode6);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(100, 200, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(200, 500, dateToTimestamp("2020-03-14T16:05:00")),
                withVnode(500, 600, dateToTimestamp("2020-03-13T17:05:00")),
                withVnode(600, 3000, dateToTimestamp("2020-03-13T16:05:00"))
        );
    }

    @Test
    public void summarizeWraparoundMultipleCoveringAndIntersectingRanges()
    {
        VnodeRepairState baseVnode = withVnode(5000, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(5000, -12000, dateToTimestamp("2020-03-13T16:00:00")); // 5000 -> 7000
        VnodeRepairState partialVnode2 = withVnode(6000, -14000, dateToTimestamp("2020-03-12T16:05:00"));
        VnodeRepairState partialVnode3 = withVnode(7000, -13000, dateToTimestamp("2020-03-12T16:05:00"));
        VnodeRepairState partialVnode4 = withVnode(7000, -10000, dateToTimestamp("2020-03-13T17:05:00")); // 7000 -> -10000
        VnodeRepairState partialVnode5 = withVnode(-15000, -12000, dateToTimestamp("2020-03-12T16:05:00"));
        VnodeRepairState partialVnode6 = withVnode(-15000, 3000, dateToTimestamp("2020-03-12T16:05:00")); // -10000 -> 3000

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1, partialVnode3, partialVnode4, partialVnode5, partialVnode6);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(5000, 7000, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(7000, -10000, dateToTimestamp("2020-03-13T17:05:00")),
                withVnode(-10000, 3000, dateToTimestamp("2020-03-12T16:05:00"))
        );
    }

    @Test
    public void summarizeWraparoundMultipleCoveringAndIntersectingRangesWithLaterRepairedAt()
    {
        VnodeRepairState baseVnode = withVnode(5000, 3000, dateToTimestamp("2020-03-12T16:00:00"));

        VnodeRepairState partialVnode1 = withVnode(5000, -12000, dateToTimestamp("2020-03-13T16:00:00")); // 5000 -> 6000
        VnodeRepairState partialVnode2 = withVnode(6000, -14000, dateToTimestamp("2020-03-14T16:05:00")); // 6000 -> -12000
        VnodeRepairState partialVnode3 = withVnode(7000, -13000, dateToTimestamp("2020-03-14T16:05:00"));
        VnodeRepairState partialVnode4 = withVnode(7000, -10000, dateToTimestamp("2020-03-13T17:05:00")); // -12000 -> -10000
        VnodeRepairState partialVnode5 = withVnode(-15000, -12000, dateToTimestamp("2020-03-14T16:05:00"));
        VnodeRepairState partialVnode6 = withVnode(-15000, 3000, dateToTimestamp("2020-03-13T16:05:00")); // -10000 -> 3000

        List<VnodeRepairState> actualVnodeRepairStates = summarize(baseVnode, partialVnode2, partialVnode1, partialVnode3, partialVnode4, partialVnode5, partialVnode6);

        assertThat(actualVnodeRepairStates).containsExactly(
                withVnode(5000, 6000, dateToTimestamp("2020-03-13T16:00:00")),
                withVnode(6000, -12000, dateToTimestamp("2020-03-14T16:05:00")),
                withVnode(-12000, -10000, dateToTimestamp("2020-03-13T17:05:00")),
                withVnode(-10000, 3000, dateToTimestamp("2020-03-13T16:05:00"))
        );
    }

    private List<VnodeRepairState> summarize(VnodeRepairState baseVnode, VnodeRepairState... partialVnodes)
    {
        return summarize(Collections.singletonList(baseVnode), partialVnodes);
    }

    private List<VnodeRepairState> summarize(List<VnodeRepairState> baseVnodes, VnodeRepairState... partialVnodes)
    {
        return VnodeRepairStateSummarizer.summarizePartialVnodes(baseVnodes, Arrays.asList(partialVnodes));
    }

    private VnodeRepairState withVnode(long start, long end, long lastRepairedAt)
    {
        return new VnodeRepairState(new LongTokenRange(start, end), ImmutableSet.of(mockHost), lastRepairedAt);
    }

    private long dateToTimestamp(String date)
    {
        return LocalDateTime.parse(date).toEpochSecond(ZoneOffset.UTC) * 1000;
    }
}
