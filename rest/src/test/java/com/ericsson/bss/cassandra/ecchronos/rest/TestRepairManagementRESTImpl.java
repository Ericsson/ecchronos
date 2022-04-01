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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView.Status;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairManagementRESTImpl
{

    @Mock
    private RepairScheduler myRepairScheduler;

    @Mock
    private OnDemandRepairScheduler myOnDemandRepairScheduler;

    private TableReferenceFactory myTableReferenceFactory = new MockTableReferenceFactory();

    private RepairManagementREST repairManagementREST;

    @Before
    public void setupMocks()
    {
        repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler,
                myTableReferenceFactory);
    }

    @Test
    public void testStatusEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.status();

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
    }

    @Test
    public void testStatusEntry()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis();

        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .build();
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.status();

        assertThat(response.getBody()).containsExactly(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testStatusMultipleEntries()
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
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withCompletedAt(3456L)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.status();

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceStatusEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.keyspaceStatus("");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceStatusNonExisting()
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;

        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(expectedLastRepairedAt)
                .withRepairInterval(expectedRepairInterval)
                .build();

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.keyspaceStatus("nonexistingkeyspace");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceStatusEntry()
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(expectedLastRepairedAt)
                .withRepairInterval(repairInterval)
                .build();
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.keyspaceStatus("ks");

        assertThat(response.getBody()).containsExactly(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceStatusMultipleEntries()
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
                .withRepairInterval(45)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withCompletedAt(3456L)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );
        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.keyspaceStatus("ks");

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTableNonExisting()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.tableStatus("ks", "tb");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTableEntry() throws UnknownHostException
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;
        Node replica = mock(Node.class);
        when(replica.getPublicAddress()).thenReturn(InetAddress.getLocalHost());

        VnodeRepairState vnodeRepairState = TestUtils
                .createVnodeRepairState(2, 3, ImmutableSet.of(replica), expectedLastRepairedAt);
        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(expectedLastRepairedAt)
                .withRepairInterval(repairInterval)
                .withVnodeRepairStateSet(ImmutableSet.of(vnodeRepairState))
                .withStatus(Status.IN_QUEUE)
                .build();

        List<ScheduledRepairJob> expectedResponse = Collections.singletonList(new ScheduledRepairJob(repairJobView));

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.tableStatus("ks", "tb");

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTableMultipleEntries() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        RepairJobView job1 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withCompletedAt(3456L)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .filter(job -> "tb".equals(job.getTableReference().getTable()))
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<List<ScheduledRepairJob>> response = repairManagementREST.tableStatus("ks", "tb");

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testIdEntry() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        UUID expectedId = UUID.randomUUID();
        RepairJobView expectedRepairJob = new TestUtils.ScheduledRepairJobBuilder()
                .withId(expectedId)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job1 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        RepairJobView job2 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(132L)
                .withRepairInterval(132)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withCompletedAt(3456L)
                .build();

        CompleteRepairJob expectedResponse = new CompleteRepairJob(expectedRepairJob);

        List<RepairJobView> repairJobViews = Arrays.asList(
                expectedRepairJob,
                job3,
                job1,
                job2
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<CompleteRepairJob> response = repairManagementREST.jobStatus(expectedId.toString());

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testIdEntryNotFound() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        RepairJobView job1 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withCompletedAt(3456L)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<CompleteRepairJob> response = repairManagementREST.jobStatus(UUID.randomUUID().toString());

        assertThat(response.getBody()).isNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    public void testIdEntryEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.emptyList());

        ResponseEntity<CompleteRepairJob> response = repairManagementREST.jobStatus(UUID.randomUUID().toString());

        assertThat(response.getBody()).isNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    public void testIdInvalidUUID()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.emptyList());

        ResponseEntity<CompleteRepairJob> response = repairManagementREST.jobStatus("123");

        assertThat(response.getBody()).isNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.config();

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testConfigEntry()
    {
        // Given
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);
        TableRepairConfig expectedResponse = new TableRepairConfig(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.config();

        assertThat(response.getBody()).containsExactly(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testConfigMultipleEntries()
    {
        // Given
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        RepairConfiguration repairConfig2 = TestUtils.createRepairConfiguration(22, 3.3, 44, 55);
        RepairJobView repairJobView2 = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks2", "tbl"), repairConfig2, null, Status.IN_QUEUE, 0);

        RepairJobView repairJobView3 = new OnDemandRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), RepairConfiguration.DEFAULT, Status.IN_QUEUE, 0,
                System.currentTimeMillis());

        List<TableRepairConfig> expectedResponse = Arrays.asList(
                new TableRepairConfig(repairJobView),
                new TableRepairConfig(repairJobView2),
                new TableRepairConfig(repairJobView3)
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(
                Arrays.asList(repairJobView, repairJobView2, repairJobView3));

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.config();

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.keyspaceConfig("");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceConfigNonExisting()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.keyspaceConfig("nonexistingkeyspace");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceConfigEntry()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        TableRepairConfig expectedResponse = new TableRepairConfig(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.keyspaceConfig("ks");

        assertThat(response.getBody()).containsExactly(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testKeyspaceConfigMultipleEntries()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        RepairConfiguration repairConfig2 = TestUtils.createRepairConfiguration(22, 3.3, 44, 55);
        RepairJobView repairJobView2 = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl2"), repairConfig2, null, Status.IN_QUEUE, 0);

        List<TableRepairConfig> expectedResponse = Arrays.asList(
                new TableRepairConfig(repairJobView),
                new TableRepairConfig(repairJobView2)
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Arrays.asList(repairJobView, repairJobView2));

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.keyspaceConfig("ks");

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTableConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.tableConfig("ks", "tbl");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTableConfigNonExisting()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.tableConfig("nonexisting", "tbl");

        assertThat(response.getBody()).isEmpty();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testTableConfigEntry()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new ScheduledRepairJobView(UUID.randomUUID(),
                myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        List<TableRepairConfig> expectedResponse = Collections.singletonList(new TableRepairConfig(repairJobView));

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        ResponseEntity<List<TableRepairConfig>> response = repairManagementREST.tableConfig("ks", "tbl");

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testConfigIdEntry() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        UUID expectedId = UUID.randomUUID();
        RepairJobView expectedRepairJob = new TestUtils.ScheduledRepairJobBuilder()
                .withId(expectedId)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job1 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        RepairJobView job2 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(132L)
                .withRepairInterval(132)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withCompletedAt(3456L)
                .build();

        TableRepairConfig expectedResponse = new TableRepairConfig(expectedRepairJob);

        List<RepairJobView> repairJobViews = Arrays.asList(
                expectedRepairJob,
                job3,
                job1,
                job2
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<TableRepairConfig> response = repairManagementREST.jobConfig(expectedId.toString());

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    public void testConfigIdEntryNotFound() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        RepairJobView job1 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        RepairJobView job3 = new TestUtils.OnDemandRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withCompletedAt(3456L)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                job3
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        ResponseEntity<TableRepairConfig> response = repairManagementREST.jobConfig(UUID.randomUUID().toString());

        assertThat(response.getBody()).isNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    public void testConfigIdEntryEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.emptyList());

        ResponseEntity<TableRepairConfig> response = repairManagementREST.jobConfig(UUID.randomUUID().toString());

        assertThat(response.getBody()).isNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    public void testConfigIdInvalidUUID()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.emptyList());

        ResponseEntity<TableRepairConfig> response = repairManagementREST.jobConfig("123");

        assertThat(response.getBody()).isNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testScheduleRepair() throws EcChronosException
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(expectedLastRepairedAt)
                .withRepairInterval(repairInterval)
                .build();
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("ks", "tb"))).thenReturn(
                repairJobView);
        ResponseEntity<ScheduledRepairJob> response = repairManagementREST.scheduleJob("ks", "tb");

        assertThat(response.getBody()).isEqualTo(expectedResponse);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
