/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleRepairManagementRESTImpl
{
    @Mock
    private RepairScheduler myRepairScheduler;

    private ScheduleRepairManagementREST scheduleRest;

    private final UUID nodeId1 = UUID.randomUUID();
    private final UUID nodeId2 = UUID.randomUUID();
    private final UUID targetJobId = UUID.randomUUID();

    private ScheduledRepairJobView job1;
    private ScheduledRepairJobView job2;
    private ScheduledRepairJobView job3;

    @Before
    public void setupMocks()
    {
        scheduleRest = new ScheduleRepairManagementRESTImpl(myRepairScheduler);

        TableReference tableRef1 = mockTableReference("ks1", "tb1");
        TableReference tableRef2 = mockTableReference("ks1", "tb2");
        TableReference tableRef3 = mockTableReference("ks2", "tb3");

        RepairConfiguration config = RepairConfiguration.newBuilder().build();

        job1 = new ScheduledRepairJobView(nodeId1, targetJobId, tableRef1, config,
                ScheduledRepairJobView.Status.ON_TIME, 0.5, 1234L, 5678L, RepairType.VNODE);
        job2 = new ScheduledRepairJobView(nodeId1, UUID.randomUUID(), tableRef2, config,
                ScheduledRepairJobView.Status.COMPLETED, 1.0, 2345L, 6789L, RepairType.VNODE);
        job3 = new ScheduledRepairJobView(nodeId2, UUID.randomUUID(), tableRef3, config,
                ScheduledRepairJobView.Status.LATE, 0.3, 3456L, 7890L, RepairType.INCREMENTAL);

        when(myRepairScheduler.getCurrentRepairJobsByNode(nodeId1)).thenReturn(Arrays.asList(job1, job2));
        when(myRepairScheduler.getCurrentRepairJobsByNode(nodeId2)).thenReturn(List.of(job3));
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Arrays.asList(job1, job2, job3));
    }

    @Test
    public void testGetAllSchedulesReturnsAllJobs()
    {
        ResponseEntity<List<Schedule>> response = scheduleRest.getSchedules(null, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Schedule> schedules = response.getBody();
        assertThat(schedules).hasSize(3);
        assertThat(schedules).extracting(s -> s.jobID)
                .containsExactlyInAnyOrder(job1.getJobId(), job2.getJobId(), job3.getJobId());
    }

    @Test
    public void testGetSchedulesByNodeAndJobIdReturnsOneResult()
    {
        ResponseEntity<Schedule> response = scheduleRest.getSchedulesByJob(
                nodeId1.toString(), targetJobId.toString(), false);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        Schedule schedule = response.getBody();
        assertThat(schedule).isNotNull();
        assertThat(schedule.nodeID).isEqualTo(nodeId1);
        assertThat(schedule.jobID).isEqualTo(targetJobId);
        assertThat(schedule.keyspace).isEqualTo("ks1");
        assertThat(schedule.table).isEqualTo("tb1");
    }

    private TableReference mockTableReference(String keyspace, String table)
    {
        TableReference ref = mock(TableReference.class);
        when(ref.getKeyspace()).thenReturn(keyspace);
        when(ref.getTable()).thenReturn(table);
        return ref;
    }
    
}
