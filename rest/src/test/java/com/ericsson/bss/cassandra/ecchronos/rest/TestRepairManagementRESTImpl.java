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
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.*;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.*;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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

    @Test
    public void testScheduledStatusEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.scheduledStatus(), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledStatusEntry()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis();

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval);
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.scheduledStatus(), scheduledRepairJobListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testScheduledStatusMultipleEntries()
    {
        List<RepairJobView> repairJobViews = Arrays.asList(
                TestUtils.createRepairJob("ks", "tb", 1234L, 11),
                TestUtils.createRepairJob("ks", "tb2", 2345L, 12)
        );

        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.scheduledStatus(), scheduledRepairJobListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testScheduledKeyspaceStatusEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceStatus(""), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledKeyspaceStatusNonExisting()
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, expectedRepairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceStatus("nonexistingkeyspace"), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledKeyspaceStatusEntry()
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, repairInterval);
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceStatus("ks"), scheduledRepairJobListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testScheduledKeyspaceStatusMultipleEntries()
    {
        List<RepairJobView> repairJobViews = Arrays.asList(
                TestUtils.createRepairJob("ks", "tb", 1234L, 11),
                TestUtils.createRepairJob("ks", "tb2", 2345L, 45)
        );
        List<ScheduledRepairJob> expectedResponse = repairJobViews.stream()
                .map(ScheduledRepairJob::new)
                .collect(Collectors.toList());

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(repairJobViews);

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceStatus("ks"), scheduledRepairJobListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testScheduledTableNonExisting()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        Map<Object, Object> response = GSON.fromJson(repairManagementREST.scheduledTableStatus("ks", "tb"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledTableEntry() throws UnknownHostException
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;
        LongTokenRange longTokenRange = new LongTokenRange(2, 3);
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());

        RepairJobView repairJobView = TestUtils.createRepairJob(UUID.randomUUID(),"ks", "tb", expectedLastRepairedAt, repairInterval,
                longTokenRange, ImmutableSet.of(host));
        CompleteRepairJob expectedResponse = new CompleteRepairJob(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        CompleteRepairJob response = GSON.fromJson(repairManagementREST.scheduledTableStatus("ks", "tb"), CompleteRepairJob.class);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testScheduledConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.scheduledConfig(), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledConfigEntry()
    {
        // Given
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl"), repairConfig, null);
        TableRepairConfig expectedResponse = new TableRepairConfig(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.scheduledConfig(), tableRepairConfigListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testScheduledConfigMultipleEntries()
    {
        // Given
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl"), repairConfig, null);

        RepairConfiguration repairConfig2 = TestUtils.createRepairConfiguration(22, 3.3, 44, 55);
        RepairJobView repairJobView2 = new RepairJobView(UUID.randomUUID(), new TableReference("ks2", "tbl"), repairConfig2, null);

        List<TableRepairConfig> expectedResponse = Arrays.asList(
                new TableRepairConfig(repairJobView),
                new TableRepairConfig(repairJobView2)
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Arrays.asList(repairJobView, repairJobView2));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.scheduledConfig(), tableRepairConfigListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testScheduledKeyspaceConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceConfig(""), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledKeyspaceConfigNonExisting()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl"), repairConfig, null);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceConfig("nonexistingkeyspace"), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledKeyspaceConfigEntry()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl"), repairConfig, null);

        TableRepairConfig expectedResponse = new TableRepairConfig(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceConfig("ks"), tableRepairConfigListType);

        assertThat(response).containsExactly(expectedResponse);
    }

    @Test
    public void testScheduledKeyspaceConfigMultipleEntries()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl"), repairConfig, null);

        RepairConfiguration repairConfig2 = TestUtils.createRepairConfiguration(22, 3.3, 44, 55);
        RepairJobView repairJobView2 = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl2"), repairConfig2, null);

        List<TableRepairConfig> expectedResponse = Arrays.asList(
                new TableRepairConfig(repairJobView),
                new TableRepairConfig(repairJobView2)
        );

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Arrays.asList(repairJobView, repairJobView2));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairManagementREST.scheduledKeyspaceConfig("ks"), tableRepairConfigListType);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testScheduledTableConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        Map<Object, Object> response = GSON.fromJson(repairManagementREST.scheduledTableConfig("ks", "tbl"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledTableConfigNonExisting()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl"), repairConfig, null);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        Map<Object, Object> response = GSON.fromJson(repairManagementREST.scheduledTableConfig("nonexisting", "tbl"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testScheduledTableConfigEntry()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(UUID.randomUUID(), new TableReference("ks", "tbl"), repairConfig, null);

        TableRepairConfig expectedResponse = new TableRepairConfig(repairJobView);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        TableRepairConfig response = GSON.fromJson(repairManagementREST.scheduledTableConfig("ks", "tbl"), TableRepairConfig.class);

        assertThat(response).isEqualTo(expectedResponse);
    }

    @Test
    public void testScheduleRepair() throws EcChronosException
    {
        RepairManagementREST repairManagementREST = new RepairManagementRESTImpl(myRepairScheduler, myOnDemandRepairScheduler);

        long expectedLastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, repairInterval);
        ScheduledRepairJob expectedResponse = new ScheduledRepairJob(repairJobView);

        when(myOnDemandRepairScheduler.scheduleJob(new TableReference("ks","tb"))).thenReturn(repairJobView);
        ScheduledRepairJob response = GSON.fromJson(repairManagementREST.scheduleJob("ks", "tb"), ScheduledRepairJob.class);
        assertThat(response).isEqualTo(expectedResponse);
    }
}
