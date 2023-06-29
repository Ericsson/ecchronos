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
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairInfo;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpStatus.BAD_REQUEST;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairManagementRESTImpl
{
    public static final int DEFAULT_GC_GRACE_SECONDS = 7200;

    @Mock
    private ReplicatedTableProvider myReplicatedTableProvider;

    @Mock
    private RepairStatsProvider myRepairStatsProvider;

    @Mock
    private TableReferenceFactory myTableReferenceFactory;

    private RepairManagementREST managementREST;

    @Before
    public void setupMocks()
    {
        managementREST = new RepairManagementRESTImpl(myTableReferenceFactory, myReplicatedTableProvider,
                myRepairStatsProvider);
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
        ResponseEntity<RepairInfo> response = managementREST.getRepairInfo(null, null, since, duration, true);

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
        ResponseEntity<RepairInfo> response = managementREST.getRepairInfo(null, null, since, null, true);

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
        ResponseEntity<RepairInfo> response = managementREST.getRepairInfo(null, null, null, duration, true);

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
        ResponseEntity<RepairInfo> response = managementREST.getRepairInfo("keyspace1", null, since, duration,
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
        ResponseEntity<RepairInfo> response = managementREST.getRepairInfo("keyspace1", "table1", since, duration,
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
            response = managementREST.getRepairInfo(null, "table1", since, duration, true);
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
        ResponseEntity<RepairInfo> response = managementREST.getRepairInfo("keyspace1", "table1", null, null,
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
            response = managementREST.getRepairInfo(null, null, null, null, true);
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
            response = managementREST.getRepairInfo("keyspace1", "table1", 0L, Duration.ofMillis(-1000), true);
        }
        catch (ResponseStatusException e)
        {
            assertThat(e.getRawStatusCode()).isEqualTo(BAD_REQUEST.value());
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
