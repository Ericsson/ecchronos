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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.OnDemandRepair;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ericsson.bss.cassandra.ecchronos.rest.TestRepairManagementRESTImpl.DEFAULT_GC_GRACE_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestOnDemandRepairManagementRESTImpl
{
    @Mock
    private OnDemandRepairScheduler myOnDemandRepairScheduler;

    @Mock
    private ReplicatedTableProvider myReplicatedTableProvider;

    @Mock
    private TableReferenceFactory myTableReferenceFactory;

    private OnDemandRepairManagementREST OnDemandRest;

    @Before
    public void setupMocks()
    {
        OnDemandRest = new OnDemandRepairManagementRESTImpl(myOnDemandRepairScheduler,
                myTableReferenceFactory, myReplicatedTableProvider);
    }

    @Test
    public void testGetNoRepairs()
    {
        when(myOnDemandRepairScheduler.getAllClusterWideRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<OnDemandRepair>> response;

        response = OnDemandRest.getRepairs(null, null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = OnDemandRest.getRepairs("ks", null, null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = OnDemandRest.getRepairs("ks", "tb", null);

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();

        response = OnDemandRest.getRepairs("ks", "tb", UUID.randomUUID().toString());

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
                3456L, RepairOptions.RepairType.INCREMENTAL);
        List<OnDemandRepairJobView> repairJobViews = Arrays.asList(job1, job2, job3);

        List<OnDemandRepair> expectedResponse = repairJobViews.stream().map(OnDemandRepair::new)
                .toList();

        when(myOnDemandRepairScheduler.getAllClusterWideRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<List<OnDemandRepair>> response = OnDemandRest.getRepairs(null, null, null);
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs(null, null, expectedHostId.toString());
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("ks", null, null);
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("ks", null, expectedHostId.toString());
        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("ks", "tb", null);
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("ks", "tb", expectedHostId.toString());
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("ks", "tb", UUID.randomUUID().toString());
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("wrong", "tb", null);
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("ks", "wrong", null);
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = OnDemandRest.getRepairs("wrong", "wrong", null);
        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testGetRepairWithId()
    {
        UUID expectedId = UUID.randomUUID();
        UUID expectedHostId = UUID.randomUUID();
        TableReference tableReference1 = mockTableReference("ks", "tb");
        OnDemandRepairJobView job1 = mockOnDemandRepairJobView(expectedId, expectedHostId, tableReference1,
                1234L, RepairOptions.RepairType.VNODE);
        TableReference tableReference2 = mockTableReference("ks", "tb2");
        OnDemandRepairJobView job2 = mockOnDemandRepairJobView(UUID.randomUUID(), expectedHostId, tableReference2,
                2345L, RepairOptions.RepairType.VNODE);
        OnDemandRepairJobView job3 = mockOnDemandRepairJobView(UUID.randomUUID(), expectedHostId, tableReference2,
                3456L, RepairOptions.RepairType.INCREMENTAL);
        List<OnDemandRepairJobView> repairJobViews = Arrays.asList(job1, job2, job3);

        List<OnDemandRepair> expectedResponse = repairJobViews.stream().map(OnDemandRepair::new)
                .toList();

        when(myOnDemandRepairScheduler.getAllClusterWideRepairJobs()).thenReturn(repairJobViews);
        ResponseEntity<List<OnDemandRepair>> response = null;
        try
        {
            response = OnDemandRest.getRepairs(UUID.randomUUID().toString(), expectedHostId.toString());
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getStatusCode().value()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();
        response = OnDemandRest.getRepairs(expectedId.toString(), expectedHostId.toString());
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        response = null;
        try
        {
            response = OnDemandRest.getRepairs(UUID.randomUUID().toString(), null);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getStatusCode().value()).isEqualTo(NOT_FOUND.value());
        }
        assertThat(response).isNull();
        response = OnDemandRest.getRepairs(expectedId.toString(), null);
        assertThat(response.getBody()).containsExactly(expectedResponse.get(0));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testRunRepair() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        TableReference tableReference = mockTableReference("ks", "tb");
        OnDemandRepairJobView localRepairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.VNODE);
        List<OnDemandRepair> localExpectedResponse = Collections.singletonList(new OnDemandRepair(localRepairJobView));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference, RepairOptions.RepairType.VNODE)).thenReturn(localRepairJobView);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = OnDemandRest.runRepair("ks", "tb", RepairOptions.RepairType.VNODE,
                true);

        assertThat(response.getBody()).isEqualTo(localExpectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        OnDemandRepairJobView repairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.VNODE);
        List<OnDemandRepair> expectedResponse = Collections.singletonList(new OnDemandRepair(repairJobView));

        when(myOnDemandRepairScheduler.scheduleClusterWideJob(tableReference, RepairOptions.RepairType.VNODE)).thenReturn(
                Collections.singletonList(repairJobView));
        response = OnDemandRest.runRepair("ks", "tb", RepairOptions.RepairType.VNODE, false);

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testRunRepairIncremental() throws EcChronosException
    {
        UUID id = UUID.randomUUID();
        UUID hostId = UUID.randomUUID();
        long completedAt = 1234L;
        TableReference tableReference = mockTableReference("ks", "tb");
        OnDemandRepairJobView localRepairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        List<OnDemandRepair> localExpectedResponse = Collections.singletonList(new OnDemandRepair(localRepairJobView));

        when(myOnDemandRepairScheduler.scheduleJob(tableReference, RepairOptions.RepairType.INCREMENTAL)).thenReturn(localRepairJobView);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = OnDemandRest.runRepair("ks", "tb", RepairOptions.RepairType.INCREMENTAL,
                true);

        assertThat(response.getBody()).isEqualTo(localExpectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        OnDemandRepairJobView repairJobView = mockOnDemandRepairJobView(id, hostId, tableReference, completedAt,
                RepairOptions.RepairType.INCREMENTAL);
        List<OnDemandRepair> expectedResponse = Collections.singletonList(new OnDemandRepair(repairJobView));

        when(myOnDemandRepairScheduler.scheduleClusterWideJob(tableReference, RepairOptions.RepairType.INCREMENTAL)).thenReturn(
                Collections.singletonList(repairJobView));
        response = OnDemandRest.runRepair("ks", "tb", RepairOptions.RepairType.INCREMENTAL, false);

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testRunRepairOnlyKeyspace() throws EcChronosException
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

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView3);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = OnDemandRest.runRepair("ks", null, null,
                true);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testRunRepairIncrementalOnlyKeyspace() throws EcChronosException
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

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView3);
        when(myReplicatedTableProvider.accept("ks")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = OnDemandRest.runRepair("ks", null, RepairOptions.RepairType.INCREMENTAL,
                true);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testRunRepairNoKeyspaceNoTable() throws EcChronosException
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

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView3);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference4, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView4);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference5, RepairOptions.RepairType.VNODE)).thenReturn(repairJobView5);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = OnDemandRest.runRepair(null, null, null,
                true);

        assertThat(response.getBody()).containsAll(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testRunRepairIncrementalNoKeyspaceNoTable() throws EcChronosException
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

        when(myOnDemandRepairScheduler.scheduleJob(tableReference1, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView1);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference2, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView2);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference3, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView3);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference4, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView4);
        when(myOnDemandRepairScheduler.scheduleJob(tableReference5, RepairOptions.RepairType.INCREMENTAL)).thenReturn(repairJobView5);
        when(myReplicatedTableProvider.accept("keyspace1")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace2")).thenReturn(true);
        when(myReplicatedTableProvider.accept("keyspace3")).thenReturn(true);
        ResponseEntity<List<OnDemandRepair>> response = OnDemandRest.runRepair(null, null, RepairOptions.RepairType.INCREMENTAL,
                true);

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
    public void testRunRepairNoKeyspaceWithTable()
    {
        ResponseEntity<List<OnDemandRepair>> response = null;
        try
        {
            response = OnDemandRest.runRepair(null, "table1", RepairOptions.RepairType.VNODE, true);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getStatusCode().value()).isEqualTo(BAD_REQUEST.value());
        }
        assertThat(response).isNull();
    }

    public TableReference mockTableReference(String keyspace, String table)
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(keyspace);
        when(tableReference.getTable()).thenReturn(table);
        when(tableReference.getGcGraceSeconds()).thenReturn(DEFAULT_GC_GRACE_SECONDS);
        when(myTableReferenceFactory.forTable(eq(keyspace), eq(table))).thenReturn(tableReference);
        return tableReference;
    }
}
