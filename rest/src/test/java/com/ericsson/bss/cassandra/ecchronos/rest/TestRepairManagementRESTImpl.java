/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairManagementRESTImpl
{
    @Mock
    private RepairScheduler myRepairScheduler;

    @Mock
    private OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Mock
    private ReplicatedTableProvider myReplicatedTableProvider;

    private TableReferenceFactory myTableReferenceFactory = new MockTableReferenceFactory();

    private RepairManagementREST repairManagementREST;

    @Before
    public void setupMocks()
    {
        repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler,
                myTableReferenceFactory, myReplicatedTableProvider);
    }

    @Test
    public void testGetNoRepairs()
    {
        when(myOnDemandRepairScheduler.getAllClusterWideRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<OnDemandRepair>> response;

        response = repairManagementREST.getRepairs(null ,null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getRepairs("ks" ,null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getRepairs("ks" ,"tb", null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getRepairs("ks" ,"tb", UUID.randomUUID().toString());

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    public void testGetRepairs()
    {
        UUID expectedId = UUID.randomUUID();
        UUID expectedHostId = UUID.randomUUID();
        RepairJobView job1 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withHostId(expectedHostId)
                .withId(expectedId)
                .withCompletedAt(1234L)
                .build();
        RepairJobView job2 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withId(UUID.randomUUID())
                .withHostId(expectedHostId)
                .withCompletedAt(2345L)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withId(UUID.randomUUID())
                .withHostId(expectedHostId)
                .withCompletedAt(3456L)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        List<OnDemandRepair> expectedResponse = repairJobViews.stream()
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());

        when(myOnDemandRepairScheduler.getAllClusterWideRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.getRepairs(null, null, null);
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs(null, null, expectedHostId.toString());
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("ks", null, null);
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("ks", null, expectedHostId.toString());
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("ks", "tb", null);
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("ks", "tb", expectedHostId.toString());
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("ks", "tb", UUID.randomUUID().toString());
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("wrong", "tb", null);
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("ks", "wrong", null);
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getRepairs("wrong", "wrong", null);
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairWithId()
    {
        UUID expectedId = UUID.randomUUID();
        UUID expectedHostId = UUID.randomUUID();
        RepairJobView job1 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withId(expectedId)
                .withHostId(expectedHostId)
                .withCompletedAt(1234L)
                .build();
        RepairJobView job2 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withId(UUID.randomUUID())
                .withHostId(expectedHostId)
                .withCompletedAt(2345L)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withId(UUID.randomUUID())
                .withHostId(expectedHostId)
                .withCompletedAt(3456L)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        List<OnDemandRepair> expectedResponse = repairJobViews.stream()
                .map(OnDemandRepair::new)
                .collect(Collectors.toList());

        when(myOnDemandRepairScheduler.getAllClusterWideRepairJobs()).thenReturn(repairJobViews);
        ResponseEntity<List<OnDemandRepair>> response = null;
        try
        {
            response = repairManagementREST.getRepairs(UUID.randomUUID().toString(), expectedHostId.toString());
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();
        response = repairManagementREST.getRepairs(expectedId.toString(), expectedHostId.toString());
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = null;
        try
        {
            response = repairManagementREST.getRepairs(UUID.randomUUID().toString(), null);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();
        response = repairManagementREST.getRepairs(expectedId.toString(), null);
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepair() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        RepairJobView localRepairJobView = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("ks")
                .withTable("tb")
                .withCompletedAt(completedAt)
                .build();
        List<OnDemandRepair> localExpectedResponse = Collections.singletonList(new OnDemandRepair(localRepairJobView));

        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("ks", "tb"))).thenReturn(
                localRepairJobView);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair("ks", "tb", true);

        assertThat(response.getBody()).isEqualTo(localExpectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        RepairJobView repairJobView = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("ks")
                .withTable("tb")
                .withCompletedAt(completedAt)
                .build();
        List<OnDemandRepair> expectedResponse = Collections.singletonList(new OnDemandRepair(repairJobView));

        when(myOnDemandRepairScheduler.scheduleClusterWideJob(myTableReferenceFactory.forTable("ks", "tb"))).thenReturn(
                Collections.singletonList(repairJobView));
        response = repairManagementREST.triggerRepair("ks", "tb", false);

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairOnlyKeyspace() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        RepairJobView repairJobView1 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("ks")
                .withTable("table1")
                .withCompletedAt(completedAt)
                .build();
        RepairJobView repairJobView2 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("ks")
                .withTable("table2")
                .withCompletedAt(completedAt)
                .build();
        RepairJobView repairJobView3 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("ks")
                .withTable("table3")
                .withCompletedAt(completedAt)
                .build();

        List<OnDemandRepair> expectedResponse = new ArrayList<>();
        expectedResponse.add(new OnDemandRepair(repairJobView1));
        expectedResponse.add(new OnDemandRepair(repairJobView2));
        expectedResponse.add(new OnDemandRepair(repairJobView3));

        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("ks", "table1"))).thenReturn(
                repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("ks", "table2"))).thenReturn(
                repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("ks", "table3"))).thenReturn(
                repairJobView3);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair("ks", null, true);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairNoKeyspaceNoTable() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        RepairJobView repairJobView1 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("keyspace1")
                .withTable("table1")
                .withCompletedAt(completedAt)
                .build();
        RepairJobView repairJobView2 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("keyspace1")
                .withTable("table2")
                .withCompletedAt(completedAt)
                .build();
        RepairJobView repairJobView3 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("keyspace1")
                .withTable("table3")
                .withCompletedAt(completedAt)
                .build();
        RepairJobView repairJobView4 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("keyspace2")
                .withTable("table4")
                .withCompletedAt(completedAt)
                .build();
        RepairJobView repairJobView5 = new TestUtils.OnDemandRepairJobBuilder()
                .withId(id)
                .withHostId(hostId)
                .withKeyspace("keyspace3")
                .withTable("table5")
                .withCompletedAt(completedAt)
                .build();

        List<OnDemandRepair> expectedResponse = new ArrayList<>();
        expectedResponse.add(new OnDemandRepair(repairJobView1));
        expectedResponse.add(new OnDemandRepair(repairJobView2));
        expectedResponse.add(new OnDemandRepair(repairJobView3));
        expectedResponse.add(new OnDemandRepair(repairJobView4));
        expectedResponse.add(new OnDemandRepair(repairJobView5));

        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("keyspace1", "table1"))).thenReturn(
                repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("keyspace1", "table2"))).thenReturn(
                repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("keyspace1", "table3"))).thenReturn(
                repairJobView3);
        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("keyspace2", "table4"))).thenReturn(
                repairJobView4);
        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("keyspace3", "table5"))).thenReturn(
                repairJobView5);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair(null, null, true);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairNoKeyspaceWithTable()
    {
        ResponseEntity<List<OnDemandRepair>> response = null;
        try
        {
            response = repairManagementREST.triggerRepair(null, "table1", true);
        }
        catch(ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(BAD_REQUEST.value());
        }
        assertThat(response).isNull();
    }

    @Test
    public void testGetNoSchedules()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<Schedule>> response;

        response = repairManagementREST.getSchedules(null ,null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getSchedules("ks" ,null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getSchedules("ks" ,"tb");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    public void testGetSchedules()
    {
        RepairJobView job1 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(2345L)
                .withRepairInterval(12)
                .build();
        RepairJobView job3 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(3333L)
                .withRepairInterval(15)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        List<Schedule> expectedResponse = repairJobViews.stream()
                .map(Schedule::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<List<Schedule>> response;

        response = repairManagementREST.getSchedules(null, null);

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);


        response = repairManagementREST.getSchedules("ks", null);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getSchedules("ks", "tb");

        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getSchedules("wrong", "tb");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getSchedules("ks", "wrong");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = repairManagementREST.getSchedules("wrong", "wrong");

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
        VnodeRepairState expectedVnodeRepairState = TestUtils
                .createVnodeRepairState(2, 3, ImmutableSet.of(replica), expectedLastRepairedAt);
        RepairJobView job1 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withId(expectedId)
                .withLastRepairedAt(1234L)
                .withVnodeRepairStateSet(ImmutableSet.of(expectedVnodeRepairState))
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withId(UUID.randomUUID())
                .withLastRepairedAt(2345L)
                .withVnodeRepairStateSet(ImmutableSet.of(expectedVnodeRepairState))
                .withRepairInterval(12)
                .build();
        RepairJobView job3 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withId(UUID.randomUUID())
                .withLastRepairedAt(2365L)
                .withVnodeRepairStateSet(ImmutableSet.of(expectedVnodeRepairState))
                .withRepairInterval(12)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        List<Schedule> expectedResponse = repairJobViews.stream()
                .map(view -> new Schedule(view, false))
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);
        ResponseEntity<Schedule> response = null;

        try
        {
            response = repairManagementREST.getSchedules(UUID.randomUUID().toString(), false);
        }
        catch(ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(NOT_FOUND.value());
        }

        assertThat(response).isNull();

        response = repairManagementREST.getSchedules(expectedId.toString(), false);

        assertThat(response.getBody()).isEqualTo(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        List<Schedule> expectedResponseFull = repairJobViews.stream()
                .map(view -> new Schedule(view, true))
                .collect(Collectors.toList());

        response = null;
        try
        {
            response = repairManagementREST.getSchedules(UUID.randomUUID().toString(), true);
        }
        catch(ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();

        response = repairManagementREST.getSchedules(expectedId.toString(), true);

        assertThat(response.getBody()).isEqualTo(expectedResponseFull.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().virtualNodeStates).isNotEmpty();
    }
}
