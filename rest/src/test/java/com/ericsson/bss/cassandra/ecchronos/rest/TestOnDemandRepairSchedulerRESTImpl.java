/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OnDemandRepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.VirtualNodeState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandRepairSchedulerRESTImpl
{
    private static final Gson GSON = new Gson();

    private static Type scheduledRepairJobListType = new TypeToken<List<ScheduledRepairJob>>()
    {
    }.getType();

    @Mock
    private OnDemandRepairScheduler myRepairScheduler;

    @Test
    public void testListEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.list(), scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testListEntry()
    {
        long lastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt,
                repairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.list(), scheduledRepairJobListType);

        assertThat(response).hasSize(1);

        ScheduledRepairJob scheduledRepairJob = response.get(0);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(0.0d);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    @Test
    public void testListKeyspaceEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.listKeyspace(""),
                scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testListKeyspaceNonExisting()
    {
        long lastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt,
                repairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.listKeyspace("nonexistingkeyspace"),
                scheduledRepairJobListType);

        assertThat(response).isEmpty();
    }

    @Test
    public void testListKeyspaceEntry()
    {
        long lastRepairedAt = 234;
        long repairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt,
                repairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = GSON.fromJson(repairSchedulerService.listKeyspace("ks"),
                scheduledRepairJobListType);

        assertThat(response).hasSize(1);

        ScheduledRepairJob scheduledRepairJob = response.get(0);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(0.0d);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    @Test
    public void testGetTableNonExisting()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        Map<Object, Object> response = GSON.fromJson(repairSchedulerService.get("ks", "tb"),
                new TypeToken<Map<Object, Object>>()
                {
                }.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testGetTableEntry() throws UnknownHostException
    {
        long lastRepairedAt = 234;
        long repairInterval = 123;
        LongTokenRange longTokenRange = new LongTokenRange(2, 3);
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt,
                repairInterval,
                longTokenRange, ImmutableSet.of(host));

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        CompleteRepairJob response = GSON.fromJson(repairSchedulerService.get("ks", "tb"), CompleteRepairJob.class);

        assertThat(response.keyspace).isEqualTo("ks");
        assertThat(response.table).isEqualTo("tb");
        assertThat(response.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(response.repairedRatio).isEqualTo(0.0d);
        assertThat(response.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
        assertThat(response.virtualNodeStates).hasSize(1);

        VirtualNodeState vnodeState = response.virtualNodeStates.get(0);
        assertThat(vnodeState.startToken).isEqualTo(longTokenRange.start);
        assertThat(vnodeState.endToken).isEqualTo(longTokenRange.end);
        assertThat(vnodeState.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(vnodeState.replicas).isEqualTo(ImmutableSet.of(InetAddress.getLocalHost()));
    }

    @Test
    public void testScheduleRepair()
    {
        OnDemandRepairSchedulerREST repairSchedulerService = new OnDemandRepairSchedulerRESTImpl(myRepairScheduler);

        String response = GSON.fromJson(repairSchedulerService.scheduleJob("ks", "tb"), String.class);
        assertThat(response).isEqualTo("Success");
    }
}
