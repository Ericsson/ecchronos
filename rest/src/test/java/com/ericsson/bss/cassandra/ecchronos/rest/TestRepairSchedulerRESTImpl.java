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
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob.Status;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.VirtualNodeState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairSchedulerRESTImpl
{
    private static final Gson GSON = new Gson();

    private static Type scheduledRepairJobListType = new TypeToken<List<ScheduledRepairJob>>(){}.getType();
    private static Type tableRepairConfigListType = new TypeToken<List<TableRepairConfig>>(){}.getType();

    @Mock
    private RepairScheduler myRepairScheduler;

    @Test
    public void testListEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.list(), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testListEntry()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis();

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.list(), scheduledRepairJobListType);

        assertThat(response).hasSize(1);

        ScheduledRepairJob scheduledRepairJob = response.get(0);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(1.0d);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
        assertThat(scheduledRepairJob.status).isEqualTo(Status.COMPLETED);
    }

    @Test
    public void testListKeyspaceEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.listKeyspace(""), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testListKeyspaceNonExisting()
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, expectedRepairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.listKeyspace("nonexistingkeyspace"), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testListKeyspaceEntry()
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, repairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.listKeyspace("ks"), scheduledRepairJobListType);

        assertThat(response).hasSize(1);

        ScheduledRepairJob scheduledRepairJob = response.get(0);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(expectedLastRepairedAt);
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(0.0d);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(expectedLastRepairedAt + repairInterval);
        assertThat(scheduledRepairJob.status).isEqualTo(Status.ERROR);
    }

    @Test
    public void testGetTableNonExisting()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        Map<Object, Object> response = GSON.fromJson(repairSchedulerService.get("ks", "tb"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testGetTableEntry() throws UnknownHostException
    {
        long expectedLastRepairedAt = 234;
        long repairInterval = 123;
        LongTokenRange longTokenRange = new LongTokenRange(2, 3);
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, repairInterval,
                longTokenRange, ImmutableSet.of(host));

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        CompleteRepairJob response = GSON.fromJson(repairSchedulerService.get("ks", "tb"), CompleteRepairJob.class);

        assertThat(response.keyspace).isEqualTo("ks");
        assertThat(response.table).isEqualTo("tb");
        assertThat(response.lastRepairedAtInMs).isEqualTo(expectedLastRepairedAt);
        assertThat(response.repairedRatio).isEqualTo(0.0d);
        assertThat(response.nextRepairInMs).isEqualTo(expectedLastRepairedAt + repairInterval);
        assertThat(response.status).isEqualTo(Status.ERROR);
        assertThat(response.virtualNodeStates).hasSize(1);

        VirtualNodeState vnodeState = response.virtualNodeStates.get(0);
        assertThat(vnodeState.startToken).isEqualTo(longTokenRange.start);
        assertThat(vnodeState.endToken).isEqualTo(longTokenRange.end);
        assertThat(vnodeState.lastRepairedAtInMs).isEqualTo(expectedLastRepairedAt);
        assertThat(vnodeState.replicas).isEqualTo(ImmutableSet.of(InetAddress.getLocalHost()));
    }

    @Test
    public void testConfigEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairSchedulerService.config(), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testConfigEntry()
    {
        // Given
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(new TableReference("ks", "tbl"), repairConfig, null);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairSchedulerService.config(), tableRepairConfigListType);

        assertThat(response).hasSize(1);

        TableRepairConfig tableRepairConfig = response.get(0);

        assertThat(tableRepairConfig.keyspace).isEqualTo("ks");
        assertThat(tableRepairConfig.table).isEqualTo("tbl");
        assertThat(tableRepairConfig.repairIntervalInMs).isEqualTo(11);
        assertThat(tableRepairConfig.repairParallelism).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        assertThat(tableRepairConfig.repairUnwindRatio).isEqualTo(2.2);
        assertThat(tableRepairConfig.repairWarningTimeInMs).isEqualTo(33);
        assertThat(tableRepairConfig.repairErrorTimeInMs).isEqualTo(44);
    }

    @Test
    public void testConfigKeyspaceEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.configKeyspace(""), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testConfigKeyspaceNonExisting()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(new TableReference("ks", "tbl"), repairConfig, null);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairSchedulerService.configKeyspace("nonexistingkeyspace"), tableRepairConfigListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testConfigKeyspaceEntry()
    {
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        RepairJobView repairJobView = new RepairJobView(new TableReference("ks", "tbl"), repairConfig, null);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<TableRepairConfig> response = GSON.fromJson(repairSchedulerService.configKeyspace("ks"), tableRepairConfigListType);

        assertThat(response).hasSize(1);

        TableRepairConfig tableRepairConfig = response.get(0);

        assertThat(tableRepairConfig.keyspace).isEqualTo("ks");
        assertThat(tableRepairConfig.table).isEqualTo("tbl");
        assertThat(tableRepairConfig.repairIntervalInMs).isEqualTo(11);
        assertThat(tableRepairConfig.repairParallelism).isEqualTo(RepairOptions.RepairParallelism.PARALLEL);
        assertThat(tableRepairConfig.repairUnwindRatio).isEqualTo(2.2);
        assertThat(tableRepairConfig.repairWarningTimeInMs).isEqualTo(33);
        assertThat(tableRepairConfig.repairErrorTimeInMs).isEqualTo(44);
    }
}
