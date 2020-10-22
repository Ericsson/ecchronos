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

import static com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView.Status;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.*;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


@RunWith(MockitoJUnitRunner.class)
public class TestRepairManagementRESTImpl
{
    private static final Gson GSON = new Gson();

    private static Type scheduledRepairJobListType = new TypeToken<List<ScheduledRepairJob>>(){}.getType();
    private static Type tableRepairConfigListType = new TypeToken<List<TableRepairConfig>>(){}.getType();

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

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.status(), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testStatusEntry()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis();

        RepairJobView repairJobView = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .build();
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.status(), scheduledRepairJobListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testStatusMultipleEntries()
    {
        RepairJobView job1 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(2345L)
                .withRepairInterval(12)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                getOnDemandRepairJobView("ks", "tb")
        );

        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.status(), scheduledRepairJobListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    private RepairJobView getOnDemandRepairJobView(String ks, String tb) {
        return new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable(ks, tb), RepairConfiguration.DEFAULT, null, Status.IN_QUEUE, 0);
    }

    @Test
    public void testKeyspaceStatusEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.keyspaceStatus(""), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testKeyspaceStatusNonExisting()
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;

        RepairJobView repairJobView = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(expectedLastRepairedAt)
                .withRepairInterval(expectedRepairInterval)
                .build();

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.keyspaceStatus("nonexistingkeyspace"), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testKeyspaceStatusEntry()
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = new TestUtils.RepairJobBuilder()
            .withKeyspace("ks")
            .withTable("tb")
            .withLastRepairedAt(expectedLastRepairedAt)
            .withRepairInterval(repairInterval)
            .build();
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.keyspaceStatus("ks"), scheduledRepairJobListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testKeyspaceStatusMultipleEntries()
    {
        RepairJobView job1 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(2345L)
                .withRepairInterval(45)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                getOnDemandRepairJobView("ks", "tb")
        );
        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.keyspaceStatus("ks"), scheduledRepairJobListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testTableNonExisting()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        Map<Object, Object> response = GSON.fromJson(repairManagementREST.tableStatus("ks", "tb"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
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
        RepairJobView repairJobView = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(expectedLastRepairedAt)
                .withRepairInterval(repairInterval)
                .withVnodeRepairStateSet(ImmutableSet.of(vnodeRepairState))
                .withStatus(Status.IN_QUEUE)
                .build();

        List<ScheduledRepairJob> expectedResponse = Collections.singletonList(new ScheduledRepairJob(repairJobView));

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.tableStatus("ks", "tb"), scheduledRepairJobListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testTableMultipleEntries() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        RepairJobView job1 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                getOnDemandRepairJobView("ks", "tb")
        );

        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .filter(job -> "tb".equals(job.getTableReference().getTable()))
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.tableStatus("ks", "tb"), scheduledRepairJobListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testIdEntry() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        UUID expectedId = UUID.randomUUID();
        RepairJobView expectedRepairJob = new TestUtils.RepairJobBuilder()
                .withId(expectedId)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job1 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        RepairJobView job2 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(132L)
                .withRepairInterval(132)
                .build();

        CompleteRepairJob expectedResponse = new CompleteRepairJob(expectedRepairJob);

        List<RepairJobView> repairJobViews = Arrays.asList(
                expectedRepairJob,
                getOnDemandRepairJobView("ks", "tb"),
                job1,
                job2
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        CompleteRepairJob response = GSON.fromJson(repairManagementREST.jobStatus("ks", "tb", expectedId.toString()), CompleteRepairJob.class);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testIdEntryNotFound() throws UnknownHostException
    {
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());
        RepairJobView job1 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(1234L)
                .withRepairInterval(11)
                .build();
        RepairJobView job2 = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb2")
                .withLastRepairedAt(134L)
                .withRepairInterval(112)
                .build();
        List<RepairJobView> repairJobViews = Arrays.asList(
                job1,
                job2,
                getOnDemandRepairJobView("ks", "tb")
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        String response = repairManagementREST.jobStatus("ks", "tb", UUID.randomUUID().toString());

        assertThat(response).isEqualTo("{}");
    }

    @Test
    public void testIdEntryEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.emptyList());

        String response = repairManagementREST.jobStatus("ks", "tb", UUID.randomUUID().toString());

        assertThat(response).isEqualTo("{}");
    }

    @Test
    public void testIdInvalidUUID()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.emptyList());

        String response = repairManagementREST.jobStatus("ks", "tb", "123");

        assertThat(response).isEqualTo("{}");
    }

    @Test
    public void testConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.config(), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testConfigEntry()
    {
        // Given
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);
        TableRepairConfig expectedResponse = new TableRepairConfig(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.config(), tableRepairConfigListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testConfigMultipleEntries()
    {
        // Given
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        RepairConfiguration repairConfig2 = TestUtils.createRepairConfiguration(22, 3.3, 44, 55);
        RepairJobView repairJobView2 = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks2", "tbl"), repairConfig2, null, Status.IN_QUEUE, 0);

        RepairJobView repairJobView3 = getOnDemandRepairJobView("ks", "tbl");

        List<TableRepairConfig> expectedResponse = Arrays.asList(
                new TableRepairConfig(repairJobView),
                new TableRepairConfig(repairJobView2),
                new TableRepairConfig(repairJobView3)
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Arrays.asList(repairJobView, repairJobView2, repairJobView3));

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.config(), tableRepairConfigListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testKeyspaceConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.keyspaceConfig(""), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testKeyspaceConfigNonExisting()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.keyspaceConfig("nonexistingkeyspace"), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testKeyspaceConfigEntry()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        TableRepairConfig expectedResponse = new TableRepairConfig(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.keyspaceConfig("ks"), tableRepairConfigListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testKeyspaceConfigMultipleEntries()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        RepairConfiguration repairConfig2 = TestUtils.createRepairConfiguration(22, 3.3, 44, 55);
        RepairJobView repairJobView2 = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl2"), repairConfig2, null, Status.IN_QUEUE, 0);

        List<TableRepairConfig> expectedResponse = Arrays.asList(
                new TableRepairConfig(repairJobView),
                new TableRepairConfig(repairJobView2)
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Arrays.asList(repairJobView, repairJobView2));

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.keyspaceConfig("ks"), tableRepairConfigListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testTableConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        Map<Object, Object> response = GSON.fromJson(repairManagementREST.tableConfig("ks", "tbl"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testTableConfigNonExisting()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        Map<Object, Object> response = GSON.fromJson(repairManagementREST.tableConfig("nonexisting", "tbl"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testTableConfigEntry()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), myTableReferenceFactory.forTable("ks", "tbl"), repairConfig, null, Status.IN_QUEUE, 0);

        List<TableRepairConfig> expectedResponse = Collections.singletonList(new TableRepairConfig(repairJobView));

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.tableConfig("ks", "tbl"), tableRepairConfigListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testScheduleRepair() throws EcChronosException
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = new TestUtils.RepairJobBuilder()
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(expectedLastRepairedAt)
                .withRepairInterval(repairInterval)
                .build();
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myOnDemandRepairScheduler.scheduleJob(myTableReferenceFactory.forTable("ks","tb"))).thenReturn(repairJobView);
        ScheduledRepairJob response = GSON.fromJson(repairManagementREST.scheduleJob("ks", "tb"), ScheduledRepairJob.class);
        assertThat(response).isEqualTo(expectedResponse);
    }
}
