/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleRepairManagementRESTImpl
{
    @Mock
    private RepairScheduler myRepairScheduler;


    private ScheduleRepairManagementREST ScheduleREST;

    @Before
    public void setupMocks()
    {
        ScheduleREST = new ScheduleRepairManagementRESTImpl(myRepairScheduler);
    }

    @Test
    public void testGetNoSchedules()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<Schedule>> response;

        response = ScheduleREST.getSchedules(null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = ScheduleREST.getSchedules("ks", null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = ScheduleREST.getSchedules("ks", "tb");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    public void testGetSchedules() throws UnknownHostException
    {
        DriverNode replica = mock(DriverNode.class);
        when(replica.getPublicAddress()).thenReturn(InetAddress.getLocalHost());
        VnodeRepairState expectedVnodeRepairState1 = new VnodeRepairState(new LongTokenRange(2, 3),
                ImmutableSet.of(replica), 1234L);
        TableReference tableReference1 = mockTableReference("ks", "tb");
        ScheduledRepairJobView job1 = mockScheduledRepairJobView(tableReference1, UUID.randomUUID(), 1234L, 11L,
                ImmutableSet.of(expectedVnodeRepairState1), RepairOptions.RepairType.VNODE);
        VnodeRepairState expectedVnodeRepairState2 = new VnodeRepairState(new LongTokenRange(2, 3),
                ImmutableSet.of(replica), 2345L);
        TableReference tableReference2 = mockTableReference("ks", "tb2");
        ScheduledRepairJobView job2 = mockScheduledRepairJobView(tableReference2, UUID.randomUUID(), 2345L, 12L,
                ImmutableSet.of(expectedVnodeRepairState2), RepairOptions.RepairType.VNODE);
        VnodeRepairState expectedVnodeRepairState3 = new VnodeRepairState(new LongTokenRange(2, 3),
                ImmutableSet.of(replica), 3333L);
        ScheduledRepairJobView job3 = mockScheduledRepairJobView(tableReference2, UUID.randomUUID(), 3333L, 15L,
                ImmutableSet.of(expectedVnodeRepairState3), RepairOptions.RepairType.VNODE);
        List<ScheduledRepairJobView> repairJobViews = Arrays.asList(job1, job2, job3);

        List<Schedule> expectedResponse = repairJobViews.stream().map(Schedule::new).toList();

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<List<Schedule>> response;

        response = ScheduleREST.getSchedules(null, null);

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = ScheduleREST.getSchedules("ks", null);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = ScheduleREST.getSchedules("ks", "tb");

        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = ScheduleREST.getSchedules("wrong", "tb");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = ScheduleREST.getSchedules("ks", "wrong");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = ScheduleREST.getSchedules("wrong", "wrong");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetScheduleWithId() throws UnknownHostException
    {
        UUID expectedId = UUID.randomUUID();
        long expectedLastRepairedAt = 234;
        DriverNode replica = mock(DriverNode.class);
        when(replica.getPublicAddress()).thenReturn(InetAddress.getLocalHost());
        VnodeRepairState expectedVnodeRepairState = new VnodeRepairState(new LongTokenRange(2, 3),
                ImmutableSet.of(replica), expectedLastRepairedAt);
        TableReference tableReference1 = mockTableReference("ks", "tb");
        ScheduledRepairJobView job1 = mockScheduledRepairJobView(tableReference1, expectedId, 1234L, 11L,
                ImmutableSet.of(expectedVnodeRepairState), RepairOptions.RepairType.VNODE);
        TableReference tableReference2 = mockTableReference("ks", "tb2");
        ScheduledRepairJobView job2 = mockScheduledRepairJobView(tableReference2, UUID.randomUUID(), 2345L, 12L,
                ImmutableSet.of(expectedVnodeRepairState), RepairOptions.RepairType.VNODE);
        ScheduledRepairJobView job3 = mockScheduledRepairJobView(tableReference2, expectedId, 2365L, 12L,
                ImmutableSet.of(expectedVnodeRepairState), RepairOptions.RepairType.VNODE);
        List<ScheduledRepairJobView> repairJobViews = Arrays.asList(job1, job2, job3);

        List<Schedule> expectedResponse = repairJobViews.stream().map(view -> new Schedule(view, false))
                .toList();

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);
        ResponseEntity<Schedule> response = null;

        try
        {
            response = ScheduleREST.getSchedules(UUID.randomUUID().toString(), false);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getStatusCode().value()).isEqualTo(NOT_FOUND.value());
        }

        assertThat(response).isNull();

        response = ScheduleREST.getSchedules(expectedId.toString(), false);

        assertThat(response.getBody()).isEqualTo(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        List<Schedule> expectedResponseFull = repairJobViews.stream().map(view -> new Schedule(view, true))
                .toList();

        response = null;
        try
        {
            response = ScheduleREST.getSchedules(UUID.randomUUID().toString(), true);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getStatusCode().value()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();

        response = ScheduleREST.getSchedules(expectedId.toString(), true);

        assertThat(response.getBody()).isEqualTo(expectedResponseFull.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().virtualNodeStates).isNotEmpty();
    }

    private ScheduledRepairJobView mockScheduledRepairJobView(TableReference tableReference, UUID id,
            long lastRepairedAt, long repairInterval, Set<VnodeRepairState> vnodeRepairState,
            RepairOptions.RepairType repairType)
    {
        RepairConfiguration repairConfigurationMock = mock(RepairConfiguration.class);
        when(repairConfigurationMock.getRepairIntervalInMs()).thenReturn(repairInterval);
        VnodeRepairStates vnodeRepairStatesMock = mock(VnodeRepairStates.class);
        when(vnodeRepairStatesMock.getVnodeRepairStates()).thenReturn(vnodeRepairState);
        RepairStateSnapshot repairStateSnapshotMock = mock(RepairStateSnapshot.class);
        when(repairStateSnapshotMock.getVnodeRepairStates()).thenReturn(vnodeRepairStatesMock);
        return new ScheduledRepairJobView(id, tableReference, repairConfigurationMock, repairStateSnapshotMock,
                ScheduledRepairJobView.Status.ON_TIME, 0.0, lastRepairedAt + repairInterval,
                repairType);
    }

    public TableReference mockTableReference(String keyspace, String table)
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(keyspace);
        when(tableReference.getTable()).thenReturn(table);
        return tableReference;
    }
}
