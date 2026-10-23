/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.UnifiedSchedule;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestUnifiedScheduleRepairManagementRESTImpl
{
    @Mock
    private RepairScheduler myRepairScheduler;

    private UnifiedScheduleRepairManagementRESTImpl myRest;

    private final UUID myNodeId1 = UUID.randomUUID();
    private final UUID myNodeId2 = UUID.randomUUID();
    private final UUID myUnifiedJobId = UUID.randomUUID();

    private ScheduledRepairJobView myUnifiedNode1;
    private ScheduledRepairJobView myUnifiedNode2;
    private ScheduledRepairJobView myLegacyJob;

    @Before
    public void setup()
    {
        myRest = new UnifiedScheduleRepairManagementRESTImpl(myRepairScheduler);

        TableReference unifiedTable = mockTableReference("ks1", "tb1");
        TableReference legacyTable = mockTableReference("ks1", "tb2");
        RepairConfiguration config = RepairConfiguration.newBuilder()
                .withRepairType(RepairType.UNIFIED_VNODE).build();

        // One unified job spanning two nodes (same jobID). Node1 ON_TIME ratio 0.8, node2 OVERDUE ratio 0.2.
        myUnifiedNode1 = new ScheduledRepairJobView(myNodeId1, myUnifiedJobId, unifiedTable, config,
                ScheduledRepairJobView.Status.ON_TIME, 0.8, 5000L, 1000L, RepairType.UNIFIED_VNODE);
        myUnifiedNode2 = new ScheduledRepairJobView(myNodeId2, myUnifiedJobId, unifiedTable, config,
                ScheduledRepairJobView.Status.OVERDUE, 0.2, 9000L, 500L, RepairType.UNIFIED_VNODE);
        // A legacy vnode job that must be excluded from the unified endpoints.
        myLegacyJob = new ScheduledRepairJobView(myNodeId1, UUID.randomUUID(), legacyTable,
                RepairConfiguration.newBuilder().build(),
                ScheduledRepairJobView.Status.COMPLETED, 1.0, 1234L, 5678L, RepairType.VNODE);

        when(myRepairScheduler.getCurrentRepairJobs())
                .thenReturn(Arrays.asList(myUnifiedNode1, myUnifiedNode2, myLegacyJob));
    }

    @Test
    public void testAggregateExcludesLegacyAndUsesWorstCaseStatusAndAverageRatio()
    {
        ResponseEntity<List<UnifiedSchedule>> response = myRest.getUnifiedSchedules(null, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<UnifiedSchedule> aggregates = response.getBody();
        assertThat(aggregates).hasSize(1);

        UnifiedSchedule aggregate = aggregates.get(0);
        assertThat(aggregate.jobID).isEqualTo(myUnifiedJobId);
        assertThat(aggregate.keyspace).isEqualTo("ks1");
        assertThat(aggregate.table).isEqualTo("tb1");
        assertThat(aggregate.nodeCount).isEqualTo(2);
        assertThat(aggregate.nodes).containsExactlyInAnyOrder(myNodeId1, myNodeId2);
        // Worst-case across nodes = OVERDUE; average ratio = (0.8 + 0.2) / 2 = 0.5.
        assertThat(aggregate.status).isEqualTo(ScheduledRepairJobView.Status.OVERDUE);
        assertThat(aggregate.repairedRatio).isEqualTo(0.5);
        // Oldest last-repaired = min(1000, 500) = 500; latest next-repair = max(5000, 9000) = 9000.
        assertThat(aggregate.lastRepairedAtInMs).isEqualTo(500L);
        assertThat(aggregate.nextRepairInMs).isEqualTo(9000L);
        assertThat(aggregate.repairType).isEqualTo(RepairType.UNIFIED_VNODE);
    }

    @Test
    public void testAggregateFilterByKeyspaceAndTable()
    {
        assertThat(myRest.getUnifiedSchedules("ks1", "tb1").getBody()).hasSize(1);
        assertThat(myRest.getUnifiedSchedules("ks1", "nope").getBody()).isEmpty();
        assertThat(myRest.getUnifiedSchedules("other", null).getBody()).isEmpty();
    }

    @Test
    public void testAggregateTableWithoutKeyspaceIsBadRequest()
    {
        assertThatThrownBy(() -> myRest.getUnifiedSchedules(null, "tb1"))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    public void testGetNodesListsAllNodeSchedulesOfJob()
    {
        ResponseEntity<List<Schedule>> response = myRest.getUnifiedScheduleNodes(myUnifiedJobId.toString());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Schedule> nodes = response.getBody();
        assertThat(nodes).hasSize(2);
        assertThat(nodes).extracting(s -> s.nodeID).containsExactlyInAnyOrder(myNodeId1, myNodeId2);
        assertThat(nodes).extracting(s -> s.jobID).containsOnly(myUnifiedJobId);
    }

    @Test
    public void testGetNodesUnknownJobIsNotFound()
    {
        assertThatThrownBy(() -> myRest.getUnifiedScheduleNodes(UUID.randomUUID().toString()))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test
    public void testGetSingleNodeReturnsThatNodeOnly()
    {
        ResponseEntity<Schedule> response =
                myRest.getUnifiedScheduleNode(myUnifiedJobId.toString(), myNodeId2.toString(), false);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Schedule schedule = response.getBody();
        assertThat(schedule).isNotNull();
        assertThat(schedule.nodeID).isEqualTo(myNodeId2);
        assertThat(schedule.jobID).isEqualTo(myUnifiedJobId);
    }

    @Test
    public void testGetSingleNodeUnknownNodeIsNotFound()
    {
        assertThatThrownBy(() ->
                myRest.getUnifiedScheduleNode(myUnifiedJobId.toString(), UUID.randomUUID().toString(), false))
                .isInstanceOf(ResponseStatusException.class);
    }

    private TableReference mockTableReference(final String keyspace, final String table)
    {
        TableReference ref = mock(TableReference.class);
        when(ref.getKeyspace()).thenReturn(keyspace);
        when(ref.getTable()).thenReturn(table);
        return ref;
    }
}
