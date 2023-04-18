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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairInfo;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.Schedule;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairManagementRESTImpl
{
    private static final int DEFAULT_GC_GRACE_SECONDS = 7200;
    @Mock
    private RepairScheduler myRepairScheduler;

    @Mock
    private OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Mock
    private ReplicatedTableProvider myReplicatedTableProvider;

    @Mock
    private RepairStatsProvider myRepairStatsProvider;

    @Mock
    private TableReferenceFactory myTableReferenceFactory;

    private RepairManagementREST repairManagementREST;

    @Before
    public void setupMocks()
    {
        repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler,
                myTableReferenceFactory, myReplicatedTableProvider, myRepairStatsProvider);
    }

    @Test
    public void testGetNoRepairs()
    {
        when(myOnDemandRepairScheduler.getAllClusterWideRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<OnDemandRepair>> response;

        response = repairManagementREST.getRepairs(null, null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getRepairs("ks", null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getRepairs("ks", "tb", null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getRepairs("ks", "tb", UUID.randomUUID().toString());

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    public void testGetRepairs()
    {
        UUID expectedId = UUID.randomUUID();
        UUID expectedHostId = UUID.randomUUID();
        TableReference tableReference1 = mockTableReference("ks", "tb");
        OnDemandRepairJobView job1 = mockOnDemandRepairJobView(expectedId, expectedHostId, tableReference1, 1234L,
                RepairOptions.RepairType.VNODE);
        TableReference tableReference2 = mockTableReference("ks", "tb2");
        OnDemandRepairJobView job2 = mockOnDemandRepairJobView(UUID.randomUUID(), expectedHostId, tableReference2,
                2345L, RepairOptions.RepairType.INCREMENTAL);
        OnDemandRepairJobView job3 = mockOnDemandRepairJobView(UUID.randomUUID(), expectedHostId, tableReference2,
                3456L, RepairOptions.RepairType.VNODE);
        List<OnDemandRepairJobView> repairJobViews = Arrays.asList(job1, job2, job3);

        List<OnDemandRepair> expectedResponse = repairJobViews.stream().map(OnDemandRepair::new)
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
        TableReference tableReference1 = mockTableReference("ks", "tb");
        OnDemandRepairJobView job1 = mockOnDemandRepairJobView(expectedId, expectedHostId, tableReference1, 1234L, RepairOptions.RepairType.VNODE);
        TableReference tableReference2 = mockTableReference("ks", "tb2");
        OnDemandRepairJobView job2 = mockOnDemandRepairJobView(UUID.randomUUID(), expectedHostId, tableReference2,
                2345L, RepairOptions.RepairType.VNODE);
        OnDemandRepairJobView job3 = mockOnDemandRepairJobView(UUID.randomUUID(), expectedHostId, tableReference2,
                3456L, RepairOptions.RepairType.INCREMENTAL);
        List<OnDemandRepairJobView> repairJobViews = Arrays.asList(job1, job2, job3);

        List<OnDemandRepair> expectedResponse = repairJobViews.stream().map(OnDemandRepair::new)
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
        TableReference tableReference = mockTableReference("ks", "tb");
        OnDemandRepairJobView localRepairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.VNODE);
        List<OnDemandRepair> localExpectedResponse = Collections.singletonList(new OnDemandRepair(localRepairJobView));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference, false)).thenReturn(localRepairJobView);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair("ks", "tb", true, false);

        assertThat(response.getBody()).isEqualTo(localExpectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        OnDemandRepairJobView repairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.VNODE);
        List<OnDemandRepair> expectedResponse = Collections.singletonList(new OnDemandRepair(repairJobView));

        when(myOnDemandRepairScheduler.scheduleClusterWideJob(tableReference, false)).thenReturn(
                Collections.singletonList(repairJobView));
        response = repairManagementREST.triggerRepair("ks", "tb", false, false);

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairIncremental() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        TableReference tableReference = mockTableReference("ks", "tb");
        OnDemandRepairJobView localRepairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        List<OnDemandRepair> localExpectedResponse = Collections.singletonList(new OnDemandRepair(localRepairJobView));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference, true)).thenReturn(localRepairJobView);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair("ks", "tb", true, true);

        assertThat(response.getBody()).isEqualTo(localExpectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        OnDemandRepairJobView repairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        List<OnDemandRepair> expectedResponse = Collections.singletonList(new OnDemandRepair(repairJobView));

        when(myOnDemandRepairScheduler.scheduleClusterWideJob(tableReference, true)).thenReturn(
                Collections.singletonList(repairJobView));
        response = repairManagementREST.triggerRepair("ks", "tb", false, true);

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairOnlyKeyspace() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        TableReference tableReference1 = mockTableReference("ks", "table1");
        OnDemandRepairJobView repairJobView1 = mockOnDemandRepairJobView(id, hostId, tableReference1, completedAt,
                RepairOptions.RepairType.VNODE);
        TableReference tableReference2 = mockTableReference("ks", "table2");
        OnDemandRepairJobView repairJobView2 = mockOnDemandRepairJobView(id, hostId, tableReference2, completedAt,
                RepairOptions.RepairType.VNODE);
        TableReference tableReference3 = mockTableReference("ks", "table3");
        OnDemandRepairJobView repairJobView3 = mockOnDemandRepairJobView(id, hostId, tableReference3, completedAt,
                RepairOptions.RepairType.VNODE);

        Set<TableReference> tableReferencesForKeyspace = new HashSet<>();
        tableReferencesForKeyspace.add(tableReference1);
        tableReferencesForKeyspace.add(tableReference2);
        tableReferencesForKeyspace.add(tableReference3);
        when(myTableReferenceFactory.forKeyspace("ks")).thenReturn(tableReferencesForKeyspace);
        List<OnDemandRepair> expectedResponse = new ArrayList<>();
        expectedResponse.add(new OnDemandRepair(repairJobView1));
        expectedResponse.add(new OnDemandRepair(repairJobView2));
        expectedResponse.add(new OnDemandRepair(repairJobView3));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, false)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, false)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, false)).thenReturn(repairJobView3);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair("ks", null, true, false);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairIncrementalOnlyKeyspace() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        TableReference tableReference1 = mockTableReference("ks", "table1");
        OnDemandRepairJobView repairJobView1 = mockOnDemandRepairJobView(id, hostId, tableReference1, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        TableReference tableReference2 = mockTableReference("ks", "table2");
        OnDemandRepairJobView repairJobView2 = mockOnDemandRepairJobView(id, hostId, tableReference2, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        TableReference tableReference3 = mockTableReference("ks", "table3");
        OnDemandRepairJobView repairJobView3 = mockOnDemandRepairJobView(id, hostId, tableReference3, completedAt,
                RepairOptions.RepairType.INCREMENTAL);

        Set<TableReference> tableReferencesForKeyspace = new HashSet<>();
        tableReferencesForKeyspace.add(tableReference1);
        tableReferencesForKeyspace.add(tableReference2);
        tableReferencesForKeyspace.add(tableReference3);
        when(myTableReferenceFactory.forKeyspace("ks")).thenReturn(tableReferencesForKeyspace);
        List<OnDemandRepair> expectedResponse = new ArrayList<>();
        expectedResponse.add(new OnDemandRepair(repairJobView1));
        expectedResponse.add(new OnDemandRepair(repairJobView2));
        expectedResponse.add(new OnDemandRepair(repairJobView3));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, true)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, true)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, true)).thenReturn(repairJobView3);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair("ks", null, true, true);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairNoKeyspaceNoTable() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        TableReference tableReference1 = mockTableReference("keyspace1", "table1");
        OnDemandRepairJobView repairJobView1 = mockOnDemandRepairJobView(id, hostId, tableReference1, completedAt,
                RepairOptions.RepairType.VNODE);
        TableReference tableReference2 = mockTableReference("keyspace1", "table2");
        OnDemandRepairJobView repairJobView2 = mockOnDemandRepairJobView(id, hostId, tableReference2, completedAt,
                RepairOptions.RepairType.VNODE);
        TableReference tableReference3 = mockTableReference("keyspace1", "table3");
        OnDemandRepairJobView repairJobView3 = mockOnDemandRepairJobView(id, hostId, tableReference3, completedAt,
                RepairOptions.RepairType.VNODE);
        TableReference tableReference4 = mockTableReference("keyspace2", "table4");
        OnDemandRepairJobView repairJobView4 = mockOnDemandRepairJobView(id, hostId, tableReference4, completedAt,
                RepairOptions.RepairType.VNODE);
        TableReference tableReference5 = mockTableReference("keyspace3", "table5");
        OnDemandRepairJobView repairJobView5 = mockOnDemandRepairJobView(id, hostId, tableReference5, completedAt,
                RepairOptions.RepairType.VNODE);
        Set<TableReference> tableReferencesForCluster = new HashSet<>();
        tableReferencesForCluster.add(tableReference1);
        tableReferencesForCluster.add(tableReference2);
        tableReferencesForCluster.add(tableReference3);
        tableReferencesForCluster.add(tableReference4);
        tableReferencesForCluster.add(tableReference5);
        when(myTableReferenceFactory.forCluster()).thenReturn(tableReferencesForCluster);

        List<OnDemandRepair> expectedResponse = new ArrayList<>();
        expectedResponse.add(new OnDemandRepair(repairJobView1));
        expectedResponse.add(new OnDemandRepair(repairJobView2));
        expectedResponse.add(new OnDemandRepair(repairJobView3));
        expectedResponse.add(new OnDemandRepair(repairJobView4));
        expectedResponse.add(new OnDemandRepair(repairJobView5));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, false)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, false)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, false)).thenReturn(repairJobView3);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference4, false)).thenReturn(repairJobView4);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference5, false)).thenReturn(repairJobView5);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair(null, null, true, false);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTriggerRepairIncrementalNoKeyspaceNoTable() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        TableReference tableReference1 = mockTableReference("keyspace1", "table1");
        OnDemandRepairJobView repairJobView1 = mockOnDemandRepairJobView(id, hostId, tableReference1, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        TableReference tableReference2 = mockTableReference("keyspace1", "table2");
        OnDemandRepairJobView repairJobView2 = mockOnDemandRepairJobView(id, hostId, tableReference2, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        TableReference tableReference3 = mockTableReference("keyspace1", "table3");
        OnDemandRepairJobView repairJobView3 = mockOnDemandRepairJobView(id, hostId, tableReference3, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        TableReference tableReference4 = mockTableReference("keyspace2", "table4");
        OnDemandRepairJobView repairJobView4 = mockOnDemandRepairJobView(id, hostId, tableReference4, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        TableReference tableReference5 = mockTableReference("keyspace3", "table5");
        OnDemandRepairJobView repairJobView5 = mockOnDemandRepairJobView(id, hostId, tableReference5, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        Set<TableReference> tableReferencesForCluster = new HashSet<>();
        tableReferencesForCluster.add(tableReference1);
        tableReferencesForCluster.add(tableReference2);
        tableReferencesForCluster.add(tableReference3);
        tableReferencesForCluster.add(tableReference4);
        tableReferencesForCluster.add(tableReference5);
        when(myTableReferenceFactory.forCluster()).thenReturn(tableReferencesForCluster);

        List<OnDemandRepair> expectedResponse = new ArrayList<>();
        expectedResponse.add(new OnDemandRepair(repairJobView1));
        expectedResponse.add(new OnDemandRepair(repairJobView2));
        expectedResponse.add(new OnDemandRepair(repairJobView3));
        expectedResponse.add(new OnDemandRepair(repairJobView4));
        expectedResponse.add(new OnDemandRepair(repairJobView5));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, true)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, true)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, true)).thenReturn(repairJobView3);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference4, true)).thenReturn(repairJobView4);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference5, true)).thenReturn(repairJobView5);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = repairManagementREST.triggerRepair(null, null, true, true);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    private OnDemandRepairJobView mockOnDemandRepairJobView(UUID id, UUID hostId, TableReference tableReference,
            Long completedAt, RepairOptions.RepairType repairType)
    {
        return new OnDemandRepairJobView(id, hostId, tableReference, OnDemandRepairJobView.Status.IN_QUEUE, 0.0,
                completedAt, repairType);
    }

    @Test
    public void testTriggerRepairNoKeyspaceWithTable()
    {
        ResponseEntity<List<OnDemandRepair>> response = null;
        try
        {
            response = repairManagementREST.triggerRepair(null, "table1", true, false);
        }
        catch (ResponseStatusException e)
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

        response = repairManagementREST.getSchedules(null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getSchedules("ks", null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = repairManagementREST.getSchedules("ks", "tb");

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

        List<Schedule> expectedResponse = repairJobViews.stream().map(Schedule::new).collect(Collectors.toList());

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
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);
        ResponseEntity<Schedule> response = null;

        try
        {
            response = repairManagementREST.getSchedules(UUID.randomUUID().toString(), false);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(NOT_FOUND.value());
        }

        assertThat(response).isNull();

        response = repairManagementREST.getSchedules(expectedId.toString(), false);

        assertThat(response.getBody()).isEqualTo(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        List<Schedule> expectedResponseFull = repairJobViews.stream().map(view -> new Schedule(view, true))
                .collect(Collectors.toList());

        response = null;
        try
        {
            response = repairManagementREST.getSchedules(UUID.randomUUID().toString(), true);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();

        response = repairManagementREST.getSchedules(expectedId.toString(), true);

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
                ScheduledRepairJobView.Status.ON_TIME, 0.0, lastRepairedAt + repairInterval, repairType);
    }

    @Test
    public void testGetRepairInfo()
    {
        long since = 1245L;
        long durationInMs = 1000L;
        Duration duration = Duration.ofMillis(durationInMs);
        long to = since + durationInMs;
        RepairStats repairStats1 = new RepairStats("keyspace1", "table1", 0.0, 0);
        TableReference tableReference1 = mockTableReference("keyspace1", "table1");
        when(myRepairStatsProvider.getRepairStats(tableReference1, since, to, true)).thenReturn(repairStats1);
        RepairStats repairStats2 = new RepairStats("keyspace1", "table2", 0.0, 0);
        TableReference tableReference2 = mockTableReference("keyspace1", "table2");
        when(myRepairStatsProvider.getRepairStats(tableReference2, since, to, true)).thenReturn(repairStats2);
        RepairStats repairStats3 = new RepairStats("keyspace1", "table3", 0.0, 0);
        TableReference tableReference3 = mockTableReference("keyspace1", "table3");
        when(myRepairStatsProvider.getRepairStats(tableReference3, since, to, true)).thenReturn(repairStats3);
        RepairStats repairStats4 = new RepairStats("keyspace2", "table4", 0.0, 0);
        TableReference tableReference4 = mockTableReference("keyspace2", "table4");
        when(myRepairStatsProvider.getRepairStats(tableReference4, since, to, true)).thenReturn(repairStats4);
        RepairStats repairStats5 = new RepairStats("keyspace3", "table5", 0.0, 0);
        TableReference tableReference5 = mockTableReference("keyspace3", "table5");
        when(myRepairStatsProvider.getRepairStats(tableReference5, since, to, true)).thenReturn(repairStats5);
        Set<TableReference> tableReferencesForCluster = new HashSet<>();
        tableReferencesForCluster.add(tableReference1);
        tableReferencesForCluster.add(tableReference2);
        tableReferencesForCluster.add(tableReference3);
        tableReferencesForCluster.add(tableReference4);
        tableReferencesForCluster.add(tableReference5);
        when(myTableReferenceFactory.forCluster()).thenReturn(tableReferencesForCluster);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        List<RepairStats> repairStats = new ArrayList<>();
        repairStats.add(repairStats1);
        repairStats.add(repairStats2);
        repairStats.add(repairStats3);
        repairStats.add(repairStats4);
        repairStats.add(repairStats5);
        RepairInfo expectedResponse = new RepairInfo(since, to, repairStats);
        ResponseEntity<RepairInfo> response = repairManagementREST.getRepairInfo(null, null, since, duration, true);

        RepairInfo returnedRepairInfo = response.getBody();
        assertThat(returnedRepairInfo).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairInfoOnlySince()
    {
        long since = 1245L;
        RepairStats repairStats1 = new RepairStats("keyspace1", "table1", 0.0, 0);
        TableReference tableReference1 = mockTableReference("keyspace1", "table1");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference1), eq(since), any(long.class),
                eq(true))).thenReturn(repairStats1);
        RepairStats repairStats2 = new RepairStats("keyspace1", "table2", 0.0, 0);
        TableReference tableReference2 = mockTableReference("keyspace1", "table2");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference2), eq(since), any(long.class),
                eq(true))).thenReturn(repairStats2);
        RepairStats repairStats3 = new RepairStats("keyspace1", "table3", 0.0, 0);
        TableReference tableReference3 = mockTableReference("keyspace1", "table3");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference3), eq(since), any(long.class),
                eq(true))).thenReturn(repairStats3);
        RepairStats repairStats4 = new RepairStats("keyspace2", "table4", 0.0, 0);
        TableReference tableReference4 = mockTableReference("keyspace2", "table4");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference4), eq(since), any(long.class),
                eq(true))).thenReturn(repairStats4);
        RepairStats repairStats5 = new RepairStats("keyspace3", "table5", 0.0, 0);
        TableReference tableReference5 = mockTableReference("keyspace3", "table5");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference5), eq(since), any(long.class),
                eq(true))).thenReturn(repairStats5);
        Set<TableReference> tableReferencesForCluster = new HashSet<>();
        tableReferencesForCluster.add(tableReference1);
        tableReferencesForCluster.add(tableReference2);
        tableReferencesForCluster.add(tableReference3);
        tableReferencesForCluster.add(tableReference4);
        tableReferencesForCluster.add(tableReference5);
        when(myTableReferenceFactory.forCluster()).thenReturn(tableReferencesForCluster);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        List<RepairStats> repairStats = new ArrayList<>();
        repairStats.add(repairStats1);
        repairStats.add(repairStats2);
        repairStats.add(repairStats3);
        repairStats.add(repairStats4);
        repairStats.add(repairStats5);
        RepairInfo expectedResponse = new RepairInfo(since, 0L, repairStats);
        ResponseEntity<RepairInfo> response = repairManagementREST.getRepairInfo(null, null, since, null, true);

        RepairInfo returnedRepairInfo = response.getBody();
        assertThat(returnedRepairInfo.repairStats).containsExactlyInAnyOrderElementsOf(expectedResponse.repairStats);
        assertThat(returnedRepairInfo.sinceInMs).isEqualTo(expectedResponse.sinceInMs);
        assertThat(returnedRepairInfo.toInMs).isGreaterThan(since);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairInfoOnlyDuration()
    {
        long durationInMs = 1000L;
        Duration duration = Duration.ofMillis(durationInMs);
        RepairStats repairStats1 = new RepairStats("keyspace1", "table1", 0.0, 0);
        TableReference tableReference1 = mockTableReference("keyspace1", "table1");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference1), any(long.class), any(long.class),
                eq(true))).thenReturn(repairStats1);
        RepairStats repairStats2 = new RepairStats("keyspace1", "table2", 0.0, 0);
        TableReference tableReference2 = mockTableReference("keyspace1", "table2");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference2), any(long.class), any(long.class),
                eq(true))).thenReturn(repairStats2);
        RepairStats repairStats3 = new RepairStats("keyspace1", "table3", 0.0, 0);
        TableReference tableReference3 = mockTableReference("keyspace1", "table3");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference3), any(long.class), any(long.class),
                eq(true))).thenReturn(repairStats3);
        RepairStats repairStats4 = new RepairStats("keyspace2", "table4", 0.0, 0);
        TableReference tableReference4 = mockTableReference("keyspace2", "table4");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference4), any(long.class), any(long.class),
                eq(true))).thenReturn(repairStats4);
        RepairStats repairStats5 = new RepairStats("keyspace3", "table5", 0.0, 0);
        TableReference tableReference5 = mockTableReference("keyspace3", "table5");
        when(myRepairStatsProvider.getRepairStats(eq(tableReference5), any(long.class), any(long.class),
                eq(true))).thenReturn(repairStats5);
        Set<TableReference> tableReferencesForCluster = new HashSet<>();
        tableReferencesForCluster.add(tableReference1);
        tableReferencesForCluster.add(tableReference2);
        tableReferencesForCluster.add(tableReference3);
        tableReferencesForCluster.add(tableReference4);
        tableReferencesForCluster.add(tableReference5);
        when(myTableReferenceFactory.forCluster()).thenReturn(tableReferencesForCluster);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        List<RepairStats> repairStats = new ArrayList<>();
        repairStats.add(repairStats1);
        repairStats.add(repairStats2);
        repairStats.add(repairStats3);
        repairStats.add(repairStats4);
        repairStats.add(repairStats5);
        ResponseEntity<RepairInfo> response = repairManagementREST.getRepairInfo(null, null, null, duration, true);

        RepairInfo returnedRepairInfo = response.getBody();
        assertThat(returnedRepairInfo.repairStats).containsExactlyInAnyOrderElementsOf(repairStats);
        assertThat(returnedRepairInfo.sinceInMs).isEqualTo(returnedRepairInfo.toInMs - durationInMs);
        assertThat(returnedRepairInfo.toInMs).isEqualTo(returnedRepairInfo.sinceInMs + durationInMs);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairInfoOnlyKeyspace() throws EcChronosException
    {
        long since = 1245L;
        long durationInMs = 1000L;
        Duration duration = Duration.ofMillis(durationInMs);
        long to = since + durationInMs;
        RepairStats repairStats1 = new RepairStats("keyspace1", "table1", 0.0, 0);
        TableReference tableReference1 = mockTableReference("keyspace1", "table1");
        when(myRepairStatsProvider.getRepairStats(tableReference1, since, to, true)).thenReturn(repairStats1);
        RepairStats repairStats2 = new RepairStats("keyspace1", "table2", 0.0, 0);
        TableReference tableReference2 = mockTableReference("keyspace1", "table2");
        when(myRepairStatsProvider.getRepairStats(tableReference2, since, to, true)).thenReturn(repairStats2);
        RepairStats repairStats3 = new RepairStats("keyspace1", "table3", 0.0, 0);
        TableReference tableReference3 = mockTableReference("keyspace1", "table3");
        when(myRepairStatsProvider.getRepairStats(tableReference3, since, to, true)).thenReturn(repairStats3);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        Set<TableReference> tableReferencesForKeyspace = new HashSet<>();
        tableReferencesForKeyspace.add(tableReference1);
        tableReferencesForKeyspace.add(tableReference2);
        tableReferencesForKeyspace.add(tableReference3);
        when(myTableReferenceFactory.forKeyspace("keyspace1")).thenReturn(tableReferencesForKeyspace);
        List<RepairStats> repairStats = new ArrayList<>();
        repairStats.add(repairStats1);
        repairStats.add(repairStats2);
        repairStats.add(repairStats3);
        RepairInfo expectedResponse = new RepairInfo(since, to, repairStats);
        ResponseEntity<RepairInfo> response = repairManagementREST.getRepairInfo("keyspace1", null, since, duration,
                true);

        RepairInfo returnedRepairInfo = response.getBody();
        assertThat(returnedRepairInfo).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairInfoKeyspaceAndTable()
    {
        long since = 1245L;
        long durationInMs = 1000L;
        Duration duration = Duration.ofMillis(durationInMs);
        long to = since + durationInMs;
        RepairStats repairStats1 = new RepairStats("keyspace1", "table1", 0.0, 0);
        TableReference tableReference1 = mockTableReference("keyspace1", "table1");
        when(myRepairStatsProvider.getRepairStats(tableReference1, since, to, true)).thenReturn(repairStats1);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        List<RepairStats> repairStats = new ArrayList<>();
        repairStats.add(repairStats1);
        RepairInfo expectedResponse = new RepairInfo(since, to, repairStats);
        ResponseEntity<RepairInfo> response = repairManagementREST.getRepairInfo("keyspace1", "table1", since, duration,
                true);

        RepairInfo returnedRepairInfo = response.getBody();
        assertThat(returnedRepairInfo).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairInfoOnlyTable()
    {
        long since = 1245L;
        long durationInMs = 1000L;
        Duration duration = Duration.ofMillis(durationInMs);
        ResponseEntity<RepairInfo> response = null;
        try
        {
            response = repairManagementREST.getRepairInfo(null, "table1", since, duration, true);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(BAD_REQUEST.value());
        }
        assertThat(response).isNull();
    }

    @Test
    public void testGetRepairInfoForKeyspaceAndTableNoSinceOrDuration()
    {
        mockTableReference("keyspace1", "table1");
        RepairStats repairStats1 = new RepairStats("keyspace1", "table1", 0.0, 0);
        when(myRepairStatsProvider.getRepairStats(any(TableReference.class), any(long.class), any(long.class),
                eq(true))).thenReturn(repairStats1);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        List<RepairStats> repairStats = new ArrayList<>();
        repairStats.add(repairStats1);
        ResponseEntity<RepairInfo> response = repairManagementREST.getRepairInfo("keyspace1", "table1", null, null,
                true);

        RepairInfo returnedRepairInfo = response.getBody();
        assertThat(returnedRepairInfo.repairStats).containsExactly(repairStats1);
        assertThat(returnedRepairInfo.toInMs - returnedRepairInfo.sinceInMs).isEqualTo(
                Duration.ofSeconds(DEFAULT_GC_GRACE_SECONDS).toMillis());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairInfoNoKeyspaceNoTableNoSinceOrDuration()
    {
        ResponseEntity<RepairInfo> response = null;
        try
        {
            response = repairManagementREST.getRepairInfo(null, null, null, null, true);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(BAD_REQUEST.value());
        }
        assertThat(response).isNull();
    }

    @Test
    public void testGetRepairInfoSinceBiggerThanSincePlusDuration()
    {
        mockTableReference("keyspace1", "table1");
        ResponseEntity<RepairInfo> response = null;
        try
        {
            response = repairManagementREST.getRepairInfo("keyspace1", "table1", 0L, Duration.ofMillis(-1000), true);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(BAD_REQUEST.value());
        }
        assertThat(response).isNull();
    }

    private TableReference mockTableReference(String keyspace, String table)
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(keyspace);
        when(tableReference.getTable()).thenReturn(table);
        when(tableReference.getGcGraceSeconds()).thenReturn(DEFAULT_GC_GRACE_SECONDS);
        when(myTableReferenceFactory.forTable(eq(keyspace), eq(table))).thenReturn(tableReference);
        return tableReference;
    }
}
