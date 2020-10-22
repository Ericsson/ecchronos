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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairEntry
{
    @Mock
    private Node mockNode;
    
    @Test
    public void testGetters()
    {
        LongTokenRange expectedLongTokenRange = new LongTokenRange(0, 1);
        long expectedStartedAt = 5;
        Set<Node> expectedParticipants = Sets.newHashSet(mockNode);
        RepairStatus expectedStatus = RepairStatus.SUCCESS;

        RepairEntry repairEntry = new RepairEntry(expectedLongTokenRange, expectedStartedAt, expectedParticipants, expectedStatus.toString());

        assertThat(repairEntry.getRange()).isEqualTo(expectedLongTokenRange);
        assertThat(repairEntry.getStartedAt()).isEqualTo(expectedStartedAt);
        assertThat(repairEntry.getParticipants()).isEqualTo(expectedParticipants);
        assertThat(repairEntry.getStatus()).isEqualTo(expectedStatus);
    }

    @Test
    public void testRepairEntriesAreEqual()
    {
        RepairEntry repairEntry = new RepairEntry(new LongTokenRange(0, 1), 5, Sets.newHashSet(mockNode), "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(new LongTokenRange(0, 1), 5, Sets.newHashSet(mockNode), "SUCCESS");

        assertThat(repairEntry).isEqualTo(repairEntry2);
        assertThat(repairEntry.hashCode()).isEqualTo(repairEntry2.hashCode());
    }

    @Test
    public void testRepairEntriesWithDifferentRangeAreNotEqual()
    {
        RepairEntry repairEntry = new RepairEntry(new LongTokenRange(0, 1), 5, Sets.newHashSet(mockNode), "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(new LongTokenRange(1, 2), 5, Sets.newHashSet(mockNode), "SUCCESS");

        assertThat(repairEntry).isNotEqualTo(repairEntry2);
    }

    @Test
    public void testRepairEntriesWithDifferentFinishedAtAreNotEqual()
    {
        RepairEntry repairEntry = new RepairEntry(new LongTokenRange(0, 1), 5, Sets.newHashSet(mockNode), "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(new LongTokenRange(0, 1), 6, Sets.newHashSet(mockNode), "SUCCESS");

        assertThat(repairEntry).isNotEqualTo(repairEntry2);
    }

    @Test
    public void testRepairEntriesWithDifferentParticipantsAreNotEqual()
    {
        RepairEntry repairEntry = new RepairEntry(new LongTokenRange(0, 1), 5, Sets.newHashSet(mockNode), "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(new LongTokenRange(0, 1), 6, Sets.newHashSet(), "SUCCESS");

        assertThat(repairEntry).isNotEqualTo(repairEntry2);
    }

    @Test
    public void testRepairEntriesWithDifferentStatusAreNotEqual()
    {
        RepairEntry repairEntry = new RepairEntry(new LongTokenRange(0, 1), 5, Sets.newHashSet(mockNode), "SUCCESS");
        RepairEntry repairEntry2 = new RepairEntry(new LongTokenRange(0, 1), 6, Sets.newHashSet(mockNode), "FAILED");

        assertThat(repairEntry).isNotEqualTo(repairEntry2);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(RepairEntry.class).usingGetClass().verify();
    }
}
